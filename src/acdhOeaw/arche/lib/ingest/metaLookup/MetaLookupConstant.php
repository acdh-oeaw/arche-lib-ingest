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

use rdfInterface\NamedNodeInterface;
use rdfInterface\DatasetNodeInterface;

/**
 * Returns a fixed set of metadata properties to every file.
 * Particularly useful for testing.
 *
 * @author zozlak
 */
class MetaLookupConstant implements MetaLookupInterface {

    private DatasetNodeInterface $metadata;

    public function __construct(DatasetNodeInterface $res) {
        $this->metadata = $res->copy();
    }

    /**
     * Searches for metadata of a given file.
     * @param string $path path to the file (just for conformance with
     *   the interface, it is not used)
     * @param array<string|NamedNodeInterface> $identifiers file's identifiers (URIs) - just for
     *   conformance with the interface, they are not used
     * @param bool $require should error be thrown when no metadata was found
     *   (not used, this class always returns metadata)
     * @return DatasetNodeInterface fetched metadata
     * @throws MetaLookupException
     */
    public function getMetadata(string $path, array $identifiers,
                                bool $require = false): DatasetNodeInterface {
        return $this->metadata->copy();
    }
}
