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

namespace acdhOeaw\arche\lib\ingest\metaLookup;

use rdfInterface\DatasetInterface;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\NamedNodeInterface;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use termTemplates\QuadTemplate as QT;
use termTemplates\PredicateTemplate as PT;
use termTemplates\NamedNodeTemplate;
use termTemplates\AnyOfTemplate;

/**
 * Searches for file metadata inside an RDF graph.
 *
 * @author zozlak
 */
class MetaLookupGraph implements MetaLookupInterface {

    /**
     * Debug flag - setting it to true causes loggin messages to be displayed.
     */
    static public bool $debug = false;

    /**
     * Graph with all metadata
     */
    private DatasetInterface $graph;
    private NamedNodeInterface $idProp;

    /**
     * Creates a MetaLookupGraph from a given dataset
     */
    public function __construct(DatasetInterface $graph,
                                NamedNodeInterface $idProp) {
        $this->graph  = $graph;
        $this->idProp = $idProp;
        $sbjs         = $this->graph->listSubjects(new QT(new NamedNodeTemplate(null, NamedNodeTemplate::ANY)));
        foreach ($sbjs as $sbj) {
            $this->graph->add(DF::quad($sbj, $this->idProp, $sbj));
        }
    }

    /**
     * Searches for metadata of a given file.
     * @param string $path path to the file (just for conformance with
     *   the interface, it is not used)
     * @param array<string|NamedNodeInterface> $identifiers file's identifiers (URIs)
     * @param bool $require should error be thrown when no metadata was found
     *   (when false a resource with no triples is returned)
     * @return DatasetNodeInterface fetched metadata
     * @throws MetaLookupException
     */
    public function getMetadata(string $path, array $identifiers,
                                bool $require = false): DatasetNodeInterface {
        $identifiers = array_map(fn($x) => $x instanceof NamedNodeInterface ? $x : DF::namedNode($x), $identifiers);
        $tmpl        = new AnyOfTemplate($identifiers);
        $candidates  = $this->graph->listSubjects(new PT($this->idProp, $tmpl));
        $candidates  = iterator_to_array($candidates);

        if (count($candidates) == 1) {
            echo self::$debug ? "  metadata found\n" : '';
            return new DatasetNode(current($candidates), $this->graph);
        } else if (count($candidates) > 1) {
            throw new MetaLookupException('more then one metadata resource');
        }

        echo self::$debug ? "  metadata not found\n" : '';
        if ($require) {
            throw new MetaLookupException('External metadata not found');
        } else {
            return new DatasetNode(current($identifiers));
        }
    }
}
