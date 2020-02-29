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

namespace acdhOeaw\acdhRepoIngest\util;

use EasyRdf\Resource;

/**
 * A simply utility class standardizing the URIs
 *
 * @author zozlak
 */
class UriNorm {

    /**
     * Set of rules as an associative array with keys being a match regex and
     * values being replace regex.
     * 
     * @var array
     */
    static public $rules;
    
    /**
     * Returns a standardized URIs.
     * 
     * If the passed URI doesn't match any rule it is returned without
     * modification.
     * @param string $uri URI to be standardized
     * @return string
     */
    static public function standardize(string $uri): string {
        foreach (self::$rules ?? [] as $match => $replace) {
            $count = 0;
            $norm = preg_replace($match, $replace, $uri, 1, $count);
            if ($count) {
                return $norm;
            }
        }
        return $uri;
    }

    /**
     * Performs id URI standardization on all id properties of a given
     * metadata resource object.
     * 
     * @param Resource $res metadata to be processed
     * @param string $property metdata property to be processed
     */
    static public function standardizeProperty(Resource $res, string $property) {
        foreach ($res->allResources($property) as $id) {
            $res->deleteResource($property, $id);
            $res->addResource($property, self::standardize((string) $id));
        }
    }
}
