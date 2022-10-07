<?php

namespace Appwrite\GraphQL;

use Appwrite\Utopia\Response;
use GraphQL\Type\Definition\ObjectType;
use GraphQL\Type\Definition\Type;
use GraphQL\Type\Schema;
use GraphQL\Utils\BuildSchema;
use GraphQL\Utils\SchemaPrinter;
use Redis;
use Swoole\Coroutine\WaitGroup;
use Swoole\FastCGI\Record\Data;
use Utopia\App;
use Utopia\Database\Database;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Route;

class SchemaBuilder
{
    /**
     * @throws \Exception
     */
    public static function buildSchema(
        App $utopia,
        Redis $cache,
        Database $dbForProject,
        string $projectId,
    ): Schema {
        $models = $utopia
            ->getResource('response')
            ->getModels();

        TypeMapper::init($models);

        App::setResource('current', static fn() => $utopia);

        $apiSchema = static::getAPISchema(
            $utopia,
            $cache,
            $projectId
        );
        $collectionSchema = static::getCollectionSchema(
            $utopia,
            $cache,
            $projectId,
            $dbForProject,
        );

        if (\is_null($collectionSchema)) {
            $queryFields = $apiSchema->getQueryType()->config['fields']();
            $mutationFields = $apiSchema->getMutationType()->config['fields']();
        } else {
            $queryFields = \array_merge_recursive(
                $apiSchema->getQueryType()->config['fields'](),
                $collectionSchema->getQueryType()->config['fields'](),
            );
            $mutationFields = \array_merge_recursive(
                $apiSchema->getMutationType()->config['fields'](),
                $collectionSchema->getMutationType()->config['fields'](),
            );
        }

        \ksort($queryFields);
        \ksort($mutationFields);

        return new Schema([
            'query' => new ObjectType([
                'name' => 'Query',
                'fields' => $queryFields
            ]),
            'mutation' => new ObjectType([
                'name' => 'Mutation',
                'fields' => $mutationFields
            ]),
        ]);
    }

    /**
     * @param Redis $cache
     * @param App $utopia
     * @param string $projectId
     * @return Schema
     * @throws \RedisException
     */
    private static function getAPISchema(
        App $utopia,
        Redis $cache,
        string $projectId,
    ): Schema {
        $schemaKey = 'graphql:api:schema';
        $versionKey = 'graphql:api:schema-version';
        $appVersion = App::getEnv('_APP_VERSION');
        $schemaVersion = $cache->get($versionKey) ?: '';
        $schemaDirty = \version_compare($appVersion, $schemaVersion, "!=");

        if ($cache->exists($schemaKey) && !$schemaDirty) {
            return BuildSchema::build($cache->get($schemaKey), static::getRestoreDecorator(
                $utopia,
                $cache,
                $projectId,
                dbForProject: null,
                type: 'api'
            ));
        }

        ResolverRegistry::clear();

        $fields = &self::buildAPISchema($utopia, $cache);

        $schema = new Schema([
            'query' => new ObjectType([
                'name' => 'Query',
                'fields' => fn() => $fields['query'],
            ]),
            'mutation' => new ObjectType([
                'name' => 'Mutation',
                'fields' => fn() => $fields['mutation'],
            ]),
        ]);

        $cache->set($schemaKey, SchemaPrinter::doPrint($schema));
        $cache->set($versionKey, $appVersion);

        return $schema;
    }

    /**
     * @param Redis $cache
     * @param App $utopia
     * @param string $projectId
     * @param Database $dbForProject
     * @return ?Schema
     * @throws \RedisException
     */
    private static function getCollectionSchema(
        App $utopia,
        Redis $cache,
        string $projectId,
        Database $dbForProject,
    ): ?Schema {
        $schemaKey = 'graphql:collections:' . $projectId . ':schema';
        $dirtyKey = 'graphql:collections:' . $projectId . ':schema-dirty';
        $dirty = $cache->get($dirtyKey);

        if ($cache->exists($schemaKey) && !$dirty) {
            \var_dump('Collection not dirty');
            $schema = $cache->get($schemaKey);
            if (empty($schema)) {
                return null;
            }
            return BuildSchema::build($schema, static::getRestoreDecorator(
                $utopia,
                $cache,
                $projectId,
                $dbForProject,
                type: 'collection',
            ));
        }

        ResolverRegistry::clear();

        $fields = &self::buildCollectionSchema(
            $utopia,
            $cache,
            $projectId,
            $dbForProject
        );

        $queries = $fields['query'];
        $mutations = $fields['mutation'];

        \var_dump('Collection dirty');

        if (empty($queries) && empty($mutations)) {
            \var_dump('Collection schema empty');
            $cache->set($schemaKey, '');
            $cache->del($dirtyKey);
            return null;
        }

        $schema = new Schema([
            'query' => new ObjectType([
                'name' => 'Query',
                'fields' => fn() => $queries,
            ]),
            'mutation' => new ObjectType([
                'name' => 'Mutation',
                'fields' => fn() => $mutations,
            ]),
        ]);

        $cache->set($schemaKey, SchemaPrinter::doPrint($schema));
        $cache->del($dirtyKey);

        return $schema;
    }

    /**
     * This function iterates all API routes and builds a GraphQL
     * schema defining types and resolvers for all response models.
     *
     * @param App $utopia
     * @param Redis $cache
     * @return array
     * @throws \Exception
     */
    private static function &buildAPISchema(
        App $utopia,
        Redis $cache
    ): array {
        $queries = [];
        $mutations = [];

        foreach (App::getRoutes() as $type => $routes) {
            foreach ($routes as $route) {
                /** @var Route $route */

                $namespace = $route->getLabel('sdk.namespace', '');
                $method = $route->getLabel('sdk.method', '');
                $name = $namespace . \ucfirst($method);

                if (empty($name)) {
                    continue;
                }

                foreach (TypeMapper::fromRoute($utopia, $cache, $route, $name) as $field) {
                    switch ($route->getMethod()) {
                        case 'GET':
                            $queries[$name] = $field;
                            break;
                        case 'POST':
                        case 'PUT':
                        case 'PATCH':
                        case 'DELETE':
                            $mutations[$name] = $field;
                            break;
                        default:
                            throw new \Exception("Unsupported method: {$route->getMethod()}");
                    }
                }
            }
        }

        $schema = [
            'query' => $queries,
            'mutation' => $mutations
        ];

        return $schema;
    }

    /**
     * Iterates all of a projects attributes and builds GraphQL
     * queries and mutations for the collections they make up.
     *
     * @param App $utopia
     * @param Redis $cache
     * @param string $projectId
     * @param Database $dbForProject
     * @return array
     * @throws \Exception
     */
    private static function &buildCollectionSchema(
        App $utopia,
        Redis $cache,
        string $projectId,
        Database $dbForProject,
    ): array {
        $collections = [];
        $queryFields = [];
        $mutationFields = [];
        $limit = 1000;
        $offset = 0;
        $count = 0;

        $wg = new WaitGroup();

        while (
            !empty($attrs = Authorization::skip(fn() => $dbForProject->find('attributes', [
            Query::limit($limit),
            Query::offset($offset),
            ])))
        ) {
            $wg->add();
            $count += count($attrs);
            \go(function () use ($utopia, $cache, $dbForProject, $projectId, &$collections, &$queryFields, &$mutationFields, $limit, &$offset, $attrs, $wg) {
                foreach ($attrs as $attr) {
                    if ($attr->getAttribute('status') !== 'available') {
                        continue;
                    }
                    $databaseId = $attr->getAttribute('databaseId');
                    $collectionId = $attr->getAttribute('collectionId');
                    $key = $attr->getAttribute('key');
                    $type = $attr->getAttribute('type');
                    $array = $attr->getAttribute('array');
                    $required = $attr->getAttribute('required');
                    $default = $attr->getAttribute('default');
                    $escapedKey = str_replace('$', '_', $key);
                    $collections[$collectionId][$escapedKey] = [
                        'type' => TypeMapper::fromCollectionAttribute(
                            $type,
                            $array,
                            $required
                        ),
                    ];
                    if ($default) {
                        $collections[$collectionId][$escapedKey]['defaultValue'] = $default;
                    }
                }

                foreach ($collections as $collectionId => $attributes) {
                    $objectType = new ObjectType([
                        'name' => \ucfirst($collectionId),
                        'fields' => \array_merge(
                            ["_id" => ['type' => Type::string()]],
                            $attributes
                        ),
                    ]);
                    $attributes = \array_merge(
                        $attributes,
                        TypeMapper::argumentsFor('mutate')
                    );

                    $queryFields[$collectionId . 'Get'] = [
                        'type' => $objectType,
                        'args' => TypeMapper::argumentsFor('id'),
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'Get',
                            utopia: $utopia,
                            cache: $cache,
                            projectId: $projectId,
                            dbForProject: $dbForProject,
                            method: 'get',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        )
                    ];
                    $queryFields[$collectionId . 'List'] = [
                        'type' => Type::listOf($objectType),
                        'args' => TypeMapper::argumentsFor('list'),
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'List',
                            utopia: $utopia,
                            cache: $cache,
                            projectId: $projectId,
                            dbForProject: $dbForProject,
                            method: 'list',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        ),
                        'complexity' => function (int $complexity, array $args) {
                            $queries = Query::parseQueries($args['queries'] ?? []);
                            $query = Query::getByType($queries, Query::TYPE_LIMIT)[0] ?? null;
                            $limit = $query ? $query->getValue() : APP_LIMIT_LIST_DEFAULT;

                            return $complexity * $limit;
                        },
                    ];

                    $mutationFields[$collectionId . 'Create'] = [
                        'type' => $objectType,
                        'args' => $attributes,
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'Create',
                            utopia: $utopia,
                            cache: $cache,
                            projectId: $projectId,
                            dbForProject: $dbForProject,
                            method: 'create',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        )
                    ];
                    $mutationFields[$collectionId . 'Update'] = [
                        'type' => $objectType,
                        'args' => \array_merge(
                            TypeMapper::argumentsFor('id'),
                            \array_map(
                                fn($attr) => $attr['type'] = Type::getNullableType($attr['type']),
                                $attributes
                            )
                        ),
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'Update',
                            utopia: $utopia,
                            cache: $cache,
                            projectId: $projectId,
                            dbForProject: $dbForProject,
                            method: 'update',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        )
                    ];
                    $mutationFields[$collectionId . 'Delete'] = [
                        'type' => TypeMapper::fromResponseModel(\ucfirst(Response::MODEL_NONE)),
                        'args' => TypeMapper::argumentsFor('id'),
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'Delete',
                            utopia: $utopia,
                            cache: $cache,
                            projectId: $projectId,
                            dbForProject: $dbForProject,
                            method: 'delete',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        )
                    ];
                }
                $wg->done();
            });
            $offset += $limit;
        }
        $wg->wait();

        $schema = [
            'query' => $queryFields,
            'mutation' => $mutationFields
        ];

        return $schema;
    }

    private static function getRestoreDecorator(
        App $utopia,
        Redis $cache,
        string $projectId,
        ?Database $dbForProject,
        string $type,
    ): callable {
        return static function (array $typeConfig) use ($utopia, $cache, $projectId, $type, $dbForProject) {
            $name = $typeConfig['name'];

            if ($name === 'Query' || $name === 'Mutation') {
                $fields = $typeConfig['fields']();

                foreach ($fields as $field => &$fieldConfig) {
                    $fieldConfig['resolve'] = ResolverRegistry::get(
                        $type,
                        $field,
                        $utopia,
                        $cache,
                        $projectId,
                        $dbForProject,
                    );
                }
                $typeConfig['fields'] = fn() => $fields;
            }
            return $typeConfig;
        };
    }
}
