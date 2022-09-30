<?php

namespace Appwrite\GraphQL;

use Appwrite\Utopia\Response;
use GraphQL\Type\Definition\ObjectType;
use GraphQL\Type\Definition\Type;
use GraphQL\Type\Schema;
use Redis;
use Swoole\Coroutine\WaitGroup;
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
        App::setResource('current', static fn() => $utopia);

        $appVersion = App::getEnv('_APP_VERSION');
        $apiSchemaKey = 'api-schema';
        $apiVersionKey = 'api-schema-version';
        $collectionSchemaKey = $projectId . '-collection-schema';
        $collectionsDirtyKey = $projectId . '-schema-dirty';
        $fullSchemaKey = $projectId . '-full-schema';

        $schemaVersion = $cache->get($apiVersionKey) ?: '';
        $collectionSchemaDirty = $cache->get($collectionsDirtyKey);
        $apiSchemaDirty = \version_compare($appVersion, $schemaVersion, "!=");

        if ($cache->exists($apiSchemaKey) && !$apiSchemaDirty) {
            $apiSchema = \json_decode($cache->get($apiSchemaKey), true);

            foreach ($apiSchema['query'] as $field => $attributes) {
                $attributes['resolve'] = ResolverRegistry::get(
                    type: 'api',
                    field: $field,
                    utopia: $utopia,
                    cache: $cache,
                );
            }
            foreach ($apiSchema['mutation'] as $field => $attributes) {
                $attributes['resolve'] = ResolverRegistry::get(
                    type: 'api',
                    field: $field,
                    utopia: $utopia,
                    cache: $cache,
                );
            }

            \var_dump('API schema loaded from cache');
        } else {
            // Not in cache or API version changed, build schema
            \var_dump('API schema not in cache or API version changed, building schema');

            $apiSchema = &self::buildAPISchema($utopia, $cache);
            $cache->set($apiSchemaKey, \json_encode($apiSchema));
            $cache->set($apiVersionKey, $appVersion);
        }

        if ($cache->exists($collectionSchemaKey) && !$collectionSchemaDirty) {
            $collectionSchema = \json_decode($cache->get($collectionSchemaKey), true);

            foreach ($collectionSchema['query'] as $field => $attributes) {
                $attributes['resolve'] = ResolverRegistry::get(
                    type: 'collection',
                    field: $field,
                    utopia: $utopia,
                    cache: $cache,
                    dbForProject: $dbForProject,
                );
            }
            foreach ($collectionSchema['mutation'] as $field => $attributes) {
                $attributes['resolve'] = ResolverRegistry::get(
                    type: 'collection',
                    field: $field,
                    utopia: $utopia,
                    cache: $cache,
                    dbForProject: $dbForProject
                );
            }

            \var_dump('Collection schema loaded from cache');
        } else {
            // Not in cache or collections changed, build schema
            \var_dump('Collection schema not in cache or collections changed, building schema');

            $collectionSchema = &self::buildCollectionSchema($utopia, $cache, $dbForProject);
            $cache->set($collectionSchemaKey, \json_encode($collectionSchema));
            $cache->del($collectionsDirtyKey);
        }

        $queryFields = \array_merge_recursive(
            $apiSchema['query'],
            $collectionSchema['query']
        );
        $mutationFields = \array_merge_recursive(
            $apiSchema['mutation'],
            $collectionSchema['mutation']
        );

        \ksort($queryFields);
        \ksort($mutationFields);

        $schema = new Schema([
            'query' => new ObjectType([
                'name' => 'Query',
                'fields' => $queryFields
            ]),
            'mutation' => new ObjectType([
                'name' => 'Mutation',
                'fields' => $mutationFields
            ])
        ]);

        return $schema;
    }

    /**
     * This function iterates all API routes and builds a GraphQL
     * schema defining types and resolvers for all response models.
     *
     * @param App $utopia
     * @return array
     * @throws \Exception
     */
    public static function &buildAPISchema(
        App $utopia,
        Redis $cache
    ): array {
        $models = $utopia
            ->getResource('response')
            ->getModels();

        TypeMapper::init($models);

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

                foreach (TypeMapper::fromRoute($utopia, $route) as $field) {
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
     * @param Database $dbForProject
     * @return array
     * @throws \Exception
     */
    public static function &buildCollectionSchema(
        App $utopia,
        Redis $cache,
        Database $dbForProject
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
            \go(function () use ($utopia, $cache, $dbForProject, &$collections, &$queryFields, &$mutationFields, $limit, &$offset, $attrs, $wg) {
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
                        'defaultValue' => $default,
                    ];
                }

                foreach ($collections as $collectionId => $attributes) {
                    $objectType = new ObjectType([
                        'name' => $collectionId,
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
                            field: $collectionId . 'Create',
                            utopia: $utopia,
                            cache: $cache,
                            dbForProject: $dbForProject,
                            method: 'create',
                            databaseId: $databaseId,
                            collectionId: $collectionId,
                        )
                    ];
                    $mutationFields[$collectionId . 'Delete'] = [
                        'type' => TypeMapper::fromResponseModel(Response::MODEL_NONE),
                        'args' => TypeMapper::argumentsFor('id'),
                        'resolve' => ResolverRegistry::get(
                            type: 'collection',
                            field: $collectionId . 'Delete',
                            utopia: $utopia,
                            cache: $cache,
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
}
