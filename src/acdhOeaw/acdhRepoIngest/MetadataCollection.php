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

namespace acdhOeaw\acdhRepoIngest;

use InvalidArgumentException;
use EasyRdf\Graph;
use EasyRdf\Resource;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
use acdhOeaw\acdhRepoLib\exception\NotFound;
use acdhOeaw\acdhRepoIngest\util\UriNorm;

/**
 * Class for importing whole metadata graph into the repository.
 *
 * @author zozlak
 */
class MetadataCollection extends Graph {

    const SKIP   = 1;
    const CREATE = 2;

    /**
     * Turns debug messages on
     * @var bool
     */
    static public $debug = false;

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
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    private $repo;

    /**
     * Parent resource for all imported graph nodes
     * @var \acdhOeaw\acdhRepoLib\RepoResource
     */
    private $resource;

    /**
     * Fedora path in the repo where imported resources are created.
     * @var string
     */
    private $fedoraLoc = '/';

    /**
     * Number of resource automatically triggering a commit (0 - no auto commit)
     * @var int
     */
    private $autoCommit = 0;

    /**
     * Used to determine when the autocommit should tak place
     * @var int
     */
    private $autoCommitCounter;

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

        $this->repo     = $repo;
        UriNorm::$rules = $this->repo->getSchema()->uriNorm ?? [];
    }

    /**
     * Sets the repository resource being parent of all resources in the
     * graph imported by the import() method.
     * 
     * @param \acdhOeaw\acdhRepoLib\RepoResource $res
     * @return \acdhOeaw\util\MetadataCollection
     * @see import()
     */
    public function setResource(RepoResource $res): MetadataCollection {
        $this->resource = $res;
        return $this;
    }

    /**
     * Sets a location where the resource will be placed.
     * 
     * Can be absolute (but will be sanitized anyway) or relative to the 
     * repository root.
     * 
     * Given location must already exist.
     * 
     * Note that this parameter is used ONLY if the resource DOES NOT EXISTS.
     * If it exists already, its location is not changed.
     * 
     * @param string $fedoraLoc fedora location 
     * @return \acdhOeaw\util\MetadataCollection
     */
    public function setFedoraLocation(string $fedoraLoc): MetadataCollection {
        $this->fedoraLoc = $fedoraLoc;
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
     * @return \acdhOeaw\util\MetadataCollection
     */
    public function setAutoCommit(int $count): MetadataCollection {
        $this->autoCommit = $count;
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
     * @return \acdhOeaw\acdhRepoLib\RepoResource[]
     * @throws InvalidArgumentException
     */
    public function import(string $namespace, int $singleOutNmsp): array {
        $idProp = $this->repo->getSchema()->id;

        $dict = [self::SKIP, self::CREATE];
        if (!in_array($singleOutNmsp, $dict)) {
            throw new InvalidArgumentException('singleOutNmsp parameters must be one of MetadataCollection::SKIP, MetadataCollection::CREATE');
        }
        $this->autoCommitCounter = 0;

        $this->removeLiteralIds();
        $this->promoteUrisToIds();
        $this->promoteBNodesToUris();
        $this->fixReferences();
        $toBeImported = $this->filterResources($namespace, $singleOutNmsp);
        //$repoResources = $this->assureIds($toBeImported);

        $repoResources = [];
        foreach ($toBeImported as $n => $res) {
            $uri = $res->getUri();
            //$repoRes = $repoResources[$uri];

            echo self::$debug ? "Importing " . $uri . " (" . ($n + 1) . "/" . count($toBeImported) . ")\n" : "";
            $this->sanitizeResource($res);

            try {
                $ids = array_map(function($x) {
                    return (string) $x;
                }, $res->allResources($idProp));
                $repoRes = $this->repo->getResourceByIds($ids);

                echo self::$debug ? "\tupdating " . $repoRes->getUri() . "\n" : "";
                $repoRes->setMetadata($res);
                $repoRes->updateMetadata();
            } catch (NotFound $ex) {
                $repoRes = $this->repo->createResource($res);
                echo self::$debug ? "\tcreated " . $repoRes->getUri() . "\n" : "";
            }

            $repoResources[] = $repoRes;
            $this->handleAutoCommit();
        }
        return array_values($repoResources);
    }

    /**
     * Returns set of resources to be imported skipping all other.
     * @param string $namespace repository resources will be created for all
     *   resources in this namespace
     * @param int $singleOutNmsp should repository resources be created
     *   representing URIs outside $namespace (MetadataCollection::SKIP or
     *   MetadataCollection::CREATE)
     * @return \acdhOeaw\acdhRepoLib\RepoResource[]
     */
    private function filterResources(string $namespace, int $singleOutNmsp): array {
        $idProp = $this->repo->getSchema()->id;
        $result = [];

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

        UriNorm::standardizeProperty($res, $idProp);

        if ($this->containsWrongRefs($res)) {
            echo $res->copy()->getGraph()->serialise('ntriples') . "\n";
            throw new InvalidArgumentException('resource contains references to blank nodes');
        }

        if (count($res->allLiterals($titleProp)) == 0) {
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

    private function handleAutoCommit(): bool {
        if ($this->autoCommit > 0) {
            $this->autoCommitCounter++;
            if ($this->autoCommitCounter >= $this->autoCommit) {
                echo self::$debug ? "Autocommit\n" : '';
                $this->repo->commit();
                $this->autoCommitCounter = 0;
                $this->repo->begin();
                return true;
            }
        }
        return false;
    }

}
