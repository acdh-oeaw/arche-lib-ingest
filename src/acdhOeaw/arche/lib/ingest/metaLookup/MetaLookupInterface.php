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

use rdfInterface\DatasetNodeInterface;
use rdfInterface\NamedNodeInterface;

/**
 * It is a common problem to couple binary data with their metadata.
 * 
 * This interface provides a method for returning metadata object based on the
 * binary file path.
 * @author zozlak
 */
interface MetaLookupInterface {

    /**
     * Returns metadata coupled with a file.
     * @param string $path path to the data file
     * @param array<string|NamedNodeInterface> $identifiers identifiers (URIs) of the file
     * @param bool $require should error be thrown when no metadata was found
     *   (when false a resource with no triples is returned)
     * @return DatasetNodeInterface fetched metadata
     */
    public function getMetadata(string $path, array $identifiers,
                                bool $require = false): DatasetNodeInterface;
}
