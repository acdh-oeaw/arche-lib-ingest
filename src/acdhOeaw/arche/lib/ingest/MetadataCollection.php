<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\lib\ingest;

use Exception;
use InvalidArgumentException;
use GuzzleHttp\Promise\RejectedPromise;
use EasyRdf\Graph;
use EasyRdf\Resource;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\exception\Conflict;
use acdhOeaw\UriNormalizer;

/**
 * Class for importing whole metadata graph into the repository.
 *
 * @author zozlak
 */
class MetadataCollection extends Graph {

    const SKIP            = 1;
    const CREATE          = 2;
    const ERRMODE_FAIL    = 'fail';
    const ERRMODE_PASS    = 'pass';
    const ERRMODE_INCLUDE = 'include';

    /**
     * Turns debug messages on
     */
    static public bool $debug = false;

    /**
     * Makes given resource a proper agent
     * 
     * @param \EasyRdf\Resource $res
     * @return \EasyRdf\Resource
     */
    static public function makeAgent(Resource $res): Resource {
        $res->addResource('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://xmlns.com/foaf/0.1/Agent');

        return $res;
    }

    /**
     * Repository connection object
     */
    private Repo $repo;

    /**
     * Parent resource for all imported graph nodes
     */
    private ?RepoResource $resource = null;

    /**
     * Should the title property be added automatically for ingested resources
     * missing it.
     */
    private bool $addTitle = false;

    /**
     * Number of resource automatically triggering a commit (0 - no auto commit)
     */
    private int $autoCommit = 0;

    /**
     * Used to determine when the autocommit should tak place
     */
    private int $autoCommitCounter = 0;

    /**
     * Is the metadata graph preprocessed already?
     */
    private bool $preprocessed = false;

    /**
     * Creates a new metadata parser.
     * 
     * @param Fedora $repo
     * @param string $file
     * @param string $format
     */
    public function __construct(Repo $repo, string $file, string $format = null) {
        parent::__construct();
        $this->parseFile($file, $format);

        $this->repo = $repo;
        UriNormalizer::init();
    }

    /**
     * Sets the repository resource being parent of all resources in the
     * graph imported by the import() method.
     * 
     * @param ?RepoResource $res
     * @return MetadataCollection
     * @see import()
     */
    public function setResource(?RepoResource $res): MetadataCollection {
        $this->resource = $res;
        return $this;
    }

    /**
     * Sets if the title property should be automatically added for ingested
     * resources which are missing it.
     * 
     * @param bool $add
     * @return MetadataCollection
     */
    public function setAddTitle(bool $add): MetadataCollection {
        $this->addTitle = $add;
        return $this;
    }

    /**
     * Controls the automatic commit behaviour.
     * 
     * Even when you use autocommit, you should commit your transaction after
     * `Indexer::index()` (the only exception is when you set auto commit to 1
     * forcing commiting each and every resource separately but you probably 
     * don't want to do that for performance reasons).
     * @param int $count number of resource automatically triggering a commit 
     *   (0 - no auto commit)
     * @return MetadataCollection
     */
    public function setAutoCommit(int $count): MetadataCollection {
        $this->autoCommit = $count;
        return $this;
    }

    /**
     * Performs preprocessing - removes literal IDs, promotes URIs to IDs, etc.
     * 
     * @return MetadataCollection
     */
    public function preprocess(): MetadataCollection {
        $this->removeLiteralIds();
        $this->promoteUrisToIds();
        $this->promoteBNodesToUris();
        $this->fixReferences();
        $this->preprocessed = true;
        return $this;
    }

    /**
     * Imports the whole graph by looping over all resources.
     * 
     * A repository resource is created for every node containing at least one 
     * cfg:fedoraIdProp property and:
     * - containg at least one other property
     * - or being within $namespace
     * - or when $singleOutNmsp equals to MetadataCollection::CREATE
     * 
     * Resources without cfg:fedoraIdProp property are skipped as we are unable
     * to identify them on the next import (which would lead to duplication).
     * 
     * Resource with a fully qualified URI is considered as having
     * cfg:fedoraIdProp (its URI is taken as cfg:fedoraIdProp property value).
     * 
     * Resources in the graph can denote relationships in any way but all
     * object URIs already existing in the repository and all object URIs in the
     * $namespace will be turned into ACDH ids.
     * 
     * @param string $namespace repository resources will be created for all
     *   resources in this namespace
     * @param int $singleOutNmsp should repository resources be created
     *   representing URIs outside $namespace (MetadataCollection::SKIP or
     *   MetadataCollection::CREATE)
     * @param string $errorMode what should happen if an error is encountered?
     *   One of:
     *   - MetadataCollection::ERRMODE_FAIL - the first encountered error throws
     *     an exception.
     *   - MetadataCollection::ERRMODE_PASS - the first encountered error turns 
     *     off the autocommit but ingestion is continued. When all resources are 
     *     processed and there was no errors, an array of RepoResource objects 
     *     is returned. If there was an error, an exception is thrown.
     *   - MetadataCollection::ERRMODE_INCLUDE - the first encountered error 
     *     turns off the autocommit but ingestion is continued. The returned 
     *     array contains RepoResource objects for successful ingestions and
     *     Exception objects for failed ones.
     * @param int $concurrency number of parallel requests to the repository
     *   allowed during the import
     * @return array<RepoResource|ClientException>
     * @throws InvalidArgumentException
     */
    public function import(string $namespace, int $singleOutNmsp,
                           string $errorMode = self::ERRMODE_FAIL,
                           int $concurrency = 3): array {
        $idProp = $this->repo->getSchema()->id;

        $dict = [self::SKIP, self::CREATE];
        if (!in_array($singleOutNmsp, $dict)) {
            throw new InvalidArgumentException('singleOutNmsp parameters must be one of MetadataCollection::SKIP, MetadataCollection::CREATE');
        }
        if (!in_array($errorMode, [self::ERRMODE_FAIL, self::ERRMODE_PASS, self::ERRMODE_INCLUDE])) {
            throw new InvalidArgumentException('errorMode parameters must be one of MetadataCollection::ERRMODE_FAIL and MetadataCollection::ERRMODE_PASS');
        }

        if (!$this->preprocessed) {
            $this->preprocess();
        }
        $toBeImported = $this->filterResources($namespace, $singleOutNmsp);

        $mapErrorMode = $errorMode === self::ERRMODE_FAIL ? Repo::REJECT_FAIL : Repo::REJECT_INCLUDE;

        // The only possible way of performing an atomic "check if resources exists and create it if not"
        // is to try to create it, therefore the algorithm goes as follows:
        // - [promise1] try to create the resource
        //   - if it succeeded, just return the repo resource
        //   - if it failed check the reason
        //     - if it's something other than primary key violation, return a RejectedPromise
        //       (which will be handled accordingly to the $mapErrorMode
        //     - it it's primary key violation such a resource exists already so:
        //       - [promise2] find it
        //         - [promise3] update its metadata
        $N = count($toBeImported);
        $n = 0;
        $f = function (Resource $res, Repo $repo) use (&$n, $N, $idProp) {
            $n++;
            $uri = $res->getUri();
            echo self::$debug ? "Importing " . $uri . " ($n/$N)\n" : "";
            $this->sanitizeResource($res);

            $promise1 = $this->repo->createResourceAsync($res);
            $promise1 = $promise1->then(
                function (RepoResource $repoRes) use ($res, $n, $N) {
                    echo self::$debug ? "\tcreated " . $repoRes->getUri() . " ($n/$N)\n" : "";
                    return $repoRes;
                }
            );
            $promise1 = $promise1->otherwise(
                function ($reason) use ($res, $idProp, $n, $N) {
                    if (!($reason instanceof Conflict)) {
                        return new RejectedPromise($reason);
                    }
                    $ids = array_map(function ($x) {
                        return (string) $x;
                    }, $res->allResources($idProp));
                    $promise2 = $this->repo->getResourceByIdsAsync($ids);
                    $promise2 = $promise2->then(
                        function (RepoResource $repoRes) use ($res, $n, $N) {
                            echo self::$debug ? "\tupdating " . $repoRes->getUri() . " ($n/$N)\n" : "";
                            $repoRes->setMetadata($res);
                            $promise3 = $repoRes->updateMetadataAsync();
                            $promise3 = $promise3->then(fn() => $repoRes);
                            return $promise3;
                        }
                    );
                    return $promise2;
                }
            );
            return $promise1;
        };

        $allRepoRes  = [];
        $errorsCount = 0;
        $chunkSize   = $this->autoCommit > 0 ? $this->autoCommit : count($toBeImported);
        for ($i = 0; $i < count($toBeImported); $i += $chunkSize) {
            if ($i > 0 && $errorsCount === 0) {
                echo self::$debug ? "Autocommit\n" : '';
                $this->repo->commit();
                $this->repo->begin();
            }
            $chunk        = array_slice($toBeImported, $i, $chunkSize);
            $chunkRepoRes = $this->repo->map($chunk, $f, $concurrency, $mapErrorMode);
            foreach ($chunkRepoRes as $j) {
                $errorsCount += (int) ($j instanceof Exception);
            }
            $allRepoRes = array_merge($allRepoRes, $chunkRepoRes);
        }
        if ($errorsCount > 0 && $errorMode === self::ERRMODE_PASS) {
            throw new IndexerException('There was at least one error during the import');
        }

        return $allRepoRes;
    }

    /**
     * Returns set of resources to be imported skipping all other.
     * @param string $namespace repository resources will be created for all
     *   resources in this namespace
     * @param int $singleOutNmsp should repository resources be created
     *   representing URIs outside $namespace (MetadataCollection::SKIP or
     *   MetadataCollection::CREATE)
     * @return array<RepoResource>
     */
    private function filterResources(string $namespace, int $singleOutNmsp): array {
        $idProp = $this->repo->getSchema()->id;
        $result = [];
        $t0     = time();

        echo self::$debug ? "Filtering resources...\n" : '';
        foreach ($this->resources() as $res) {
            echo self::$debug ? "\t" . $res->getUri() . "\n" : '';

            $nonIdProps = array_diff($res->propertyUris(), [$idProp]);
            $inNmsp     = false;
            $ids        = [];
            foreach ($res->allResources($idProp) as $id) {
                $id     = (string) $id;
                $ids[]  = $id;
                $inNmsp = $inNmsp || strpos($id, $namespace) === 0;
            }

            if (count($ids) == 0) {
                echo self::$debug ? "\t\tskipping - no ids\n" : '';
            } elseif (count($nonIdProps) == 0 && $this->isIdElsewhere($res)) {
                echo self::$debug ? "\t\tskipping - single id assigned to another resource\n" : '';
            } elseif (count($nonIdProps) == 0 && $singleOutNmsp !== MetadataCollection::CREATE && !$inNmsp) {
                echo self::$debug ? "\t\tskipping - onlyIds, outside namespace and mode == MetadataCollection::SKIP\n" : '';
            } else {
                echo self::$debug ? "\t\tincluding\n" : '';
                $result[] = $res;
            }

            $t1 = time();
            if ($t1 > $t0 && $this->repo->inTransaction()) {
                $this->repo->prolong();
                $t0 = $t1;
            }
        }

        return $result;
    }

    /**
     * Checks if a given resource is a schema:id of some other node in
     * the graph.
     * 
     * @param Resource $res
     * @return bool
     */
    private function isIdElsewhere(Resource $res): bool {
        $revMatches = $this->reversePropertyUris($res);
        foreach ($revMatches as $prop) {
            if ($prop !== $this->repo->getSchema()->id) {
                continue;
            }
            $matches = $this->resourcesMatching($prop, $res);
            foreach ($matches as $i) {
                if ($i->getUri() != $res->getUri()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * To avoid creation of duplicated resources it must be assured every
     * resource is referenced acrossed the whole graph with only one URI
     * 
     * As it doesn't matter which exactly, the resource URI itself is
     * a convenient choice
     * 
     * @return void
     */
    private function fixReferences(): void {
        echo self::$debug ? "Fixing references...\n" : '';
        $idProp = $this->repo->getSchema()->id;
        // collect id => uri mappings
        $map    = [];
        foreach ($this->resources() as $i) {
            foreach ($i->allResources($idProp) as $v) {
                $map[(string) $v] = (string) $i;
            }
        }
        // fix references
        foreach ($this->resources() as $i) {
            $properties = array_diff($i->propertyUris(), [$idProp]);
            foreach ($properties as $p) {
                foreach ($i->allResources($p) as $v) {
                    $vv = (string) $v;
                    if (isset($map[$vv]) && $map[$vv] !== $vv) {
                        echo self::$debug ? "\t$vv => " . $map[$vv] . "\n" : '';
                        $i->delete($p, $v);
                        $i->addResource($p, $map[$vv]);
                    }
                }
            }
        }
    }

    /**
     * Checks if a node contains wrong edges (references to blank nodes).
     * 
     * @param Resource $res
     * @return bool
     */
    private function containsWrongRefs(Resource $res): bool {
        $properties = array_diff($res->propertyUris(), [$this->repo->getSchema()->id]);
        foreach ($properties as $prop) {
            foreach ($res->allResources($prop) as $val) {
                if ($val->isBNode()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Promotes BNodes to their first schema:id and fixes references to them.
     */
    private function promoteBNodesToUris() {
        echo self::$debug ? "Promoting BNodes to URIs...\n" : '';

        $idProp = $this->repo->getSchema()->id;
        $map    = [];
        foreach ($this->resources() as $i) {
            $id = $i->getResource($idProp);
            if ($i->isBNode() && $id !== null) {
                echo self::$debug ? "\t" . $i->getUri() . " => " . $id->getUri() . "\n" : '';
                $map[$i->getUri()] = $id;
                foreach ($i->propertyUris() as $p) {
                    foreach ($i->all($p) as $v) {
                        $id->add($p, $v);
                        $i->delete($p, $v);
                    }
                }
            }
        }
        foreach ($this->resources() as $i) {
            foreach ($i->propertyUris() as $p) {
                foreach ($i->allResources($p) as $v) {
                    if (isset($map[$v->getUri()])) {
                        $i->delete($p, $v);
                        $i->addResource($p, $map[$v->getUri()]);
                    }
                }
            }
        }
    }

    /**
     * Promotes subjects being fully qualified URLs to ids.
     */
    private function promoteUrisToIds(): void {
        echo self::$debug ? "Promoting URIs to ids...\n" : '';
        foreach ($this->resources() as $i) {
            if (!$i->isBNode() and count($i->propertyUris()) > 0) {
                $uri = (string) $i;
                echo self::$debug ? "\t" . $uri . "\n" : '';
                $i->addResource($this->repo->getSchema()->id, $uri);
            }
        }
    }

    /**
     * Cleans up resource metadata.
     * 
     * @param Resource $res
     * @return \EasyRdf\Resource
     * @throws InvalidArgumentException
     */
    private function sanitizeResource(Resource $res): Resource {
        $idProp     = $this->repo->getSchema()->id;
        $titleProp  = $this->repo->getSchema()->label;
        $relProp    = $this->repo->getSchema()->parent;
        $nonIdProps = array_diff($res->propertyUris(), [$idProp]);
        // don't do anything when it's purely-id resource
        if (count($nonIdProps) == 0) {
            return $res;
        }

        foreach ($res->propertyUris() as $prop) {
            // because every triple object creates a repo resource and therefore ends up as an id
            UriNormalizer::gNormalizeMeta($res, $prop);
        }

        if ($this->containsWrongRefs($res)) {
            echo $res->copy()->getGraph()->serialise('ntriples') . "\n";
            throw new InvalidArgumentException('resource contains references to blank nodes');
        }

        if ($this->addTitle && count($res->allLiterals($titleProp)) === 0) {
            $res->addLiteral($titleProp, $res->getResource($idProp), 'en');
        }

        if ($res->isA('http://xmlns.com/foaf/0.1/Person') || $res->isA('http://xmlns.com/foaf/0.1/Agent')) {
            $res = self::makeAgent($res);
        }

        if ($this->resource !== null) {
            $res->addResource($relProp, $this->resource->getUri());
        }

        return $res;
    }

    /**
     * Removes literal ids from the graph.
     */
    private function removeLiteralIds(): void {
        echo self::$debug ? "Removing literal ids...\n" : "";
        $idProp = $this->repo->getSchema()->id;

        foreach ($this->resources() as $i) {
            foreach ($i->allLiterals($idProp) as $j) {
                $i->delete($idProp, $j);
                if (self::$debug) {
                    echo "\tremoved " . $j . " from " . $i->getUri() . "\n";
                }
            }
        }
    }
}
