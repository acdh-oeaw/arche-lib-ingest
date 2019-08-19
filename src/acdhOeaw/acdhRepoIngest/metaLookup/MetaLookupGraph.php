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

namespace acdhOeaw\acdhRepoIngest\metaLookup;

use EasyRdf\Resource;
use EasyRdf\Graph;

/**
 * Searches for file metadata inside an RDF graph.
 *
 * @author zozlak
 */
class MetaLookupGraph implements MetaLookupInterface {

    /**
     * Debug flag - setting it to true causes loggin messages to be displayed.
     * @var bool
     */
    static public $debug = false;

    /**
     * Graph with all metadata
     * @var \EasyRdf\Graph
     */
    private $graph;

    /**
     *
     * @var string
     */
    private $idProp;

    /**
     * Creates a MetaLookupGraph from a given EasyRdf\Graph
     * @param \EasyRdf\Graph $graph metadata graph
     */
    public function __construct(Graph $graph, string $idProp) {
        $this->graph  = $graph;
        $this->idProp = $idProp;
        foreach ($this->graph->resources() as $i) {
            if (!$i->isBNode() && count($i->properties()) > 0) {
                $i->addResource($this->idProp, $i->getUri());
            }
        }
    }

    /**
     * Searches for metadata of a given file.
     * @param string $path path to the file (just for conformance with
     *   the interface, it is not used)
     * @param \EasyRdf\Resource $meta file's metadata 
     * @param bool $require should error be thrown when no metadata was found
     *   (when false a resource with no triples is returned)
     * @return \EasyRdf\Resource fetched metadata
     * @throws MetaLookupException
     */
    public function getMetadata(string $path, Resource $meta,
                                bool $require = false): Resource {
        if ($meta == null) {
            return(new Graph())->resource('.');
        }

        $candidates = [];
        foreach ($meta->allResources($this->idProp) as $id) {
            foreach ($this->graph->resourcesMatching($this->idProp, $id) as $i) {
                $candidates[$i->getUri()] = $i;
            }
        }

        if (count($candidates) == 1) {
            echo self::$debug ? "  metadata found\n" : '';
            return array_pop($candidates);
        } else if (count($candidates) > 1) {
print_r(array_keys($candidates));
            throw new MetaLookupException('more then one metadata resource');
        }

        echo self::$debug ? "  metadata not found\n" : '';
        if ($require) {
            throw new MetaLookupException('External metadata not found');
        } else {
            return(new Graph())->resource('.');
        }
    }

}
