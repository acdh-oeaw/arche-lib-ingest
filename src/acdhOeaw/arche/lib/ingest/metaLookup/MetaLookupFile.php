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

use InvalidArgumentException;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\NamedNodeInterface;
use quickRdf\DataFactory as DF;
use quickRdf\Dataset;
use quickRdf\DatasetNode;
use quickRdfIo\Util as RdfUtil;

/**
 * Implements metadata lookup by searching in a given metadata locations for
 * a file with an original file name with a given extension appended.
 *
 * @author zozlak
 */
class MetaLookupFile implements MetaLookupInterface {

    /**
     * Debug flag - setting it to true causes loggin messages to be displayed.
     * @var bool
     */
    static public $debug = false;

    /**
     * Array of possible metadata locations (relative and/or absolute)
     * @var array<string>
     */
    private array $locations;

    /**
     * Suffix added to a file name to form a metadata file name.
     */
    private string $extension;

    /**
     * Metadata file format
     */
    private ?string $format;

    /**
     * Creates a new MetaLookupFile instance.
     * @param string[] $locations location to search for metadata files in
     *   (both absolute and relative paths allowed)
     * @param string $extension suffix added to the original filename to form
     *   a metadata file name
     * @param string $format metadata format understandable for 
     *   \EasyRdf\Graph::parseFile() (if null format will be autodetected)
     */
    public function __construct(array $locations = [],
                                string $extension = '.ttl',
                                ?string $format = null) {
        $this->locations = $locations;
        $this->extension = $extension;
        $this->format    = $format;
    }

    /**
     * Searches for metadata of a given file.
     * @param string $path path to the file
     * @param array<string|NamedNodeInterface> $identifiers file's identifiers (URIs) - just for 
     *   conformance with the interface, they are not used
     * @param bool $require should error be thrown when no metadata was found
     *   (when false a resource with no triples is returned)
     * @return DatasetNodeInterface fetched metadata
     * @throws \InvalidArgumentException
     * @throws MetaLookupException
     */
    public function getMetadata(string $path, array $identifiers,
                                bool $require = false): DatasetNodeInterface {
        if (!file_exists($path)) {
            throw new InvalidArgumentException('no such file');
        }
        $dir  = dirname($path);
        $name = basename($path) . $this->extension;

        foreach ($this->locations as $loc) {
            if (substr($loc, 0, 1) !== '/') {
                $loc = $dir . '/' . $loc;
            }
            $loc = $loc . '/' . $name;

            echo self::$debug ? '  trying metadata location ' . $loc . "...\n" : '';
            if (file_exists($loc)) {
                echo self::$debug ? "    found\n" : '';

                $graph      = new Dataset();
                $graph->add(RdfUtil::parse($loc, new DF(), $this->format));
                $candidates = iterator_to_array($graph->listSubjects());

                if (count($candidates) == 1) {
                    return (new DatasetNode(current($candidates)))->withDataset($graph);
                } else if (count($candidates) > 1) {
                    throw new MetaLookupException('more then one metadata resource');
                } else {
                    echo self::$debug ? "      but no metadata inside\n" : '';
                }
            } else {
                echo self::$debug ? "    NOT found\n" : '';
            }
        }

        if ($require) {
            throw new MetaLookupException('External metadata not found', 11);
        } else {
            return new DatasetNode(current($identifiers));
        }
    }
}
