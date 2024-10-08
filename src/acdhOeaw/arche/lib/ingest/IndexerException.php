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
use Throwable;
use acdhOeaw\arche\lib\RepoResource;

/**
 * Exception used by the Indexer class, giving access to resources which were
 * commited when an error occured.
 *
 * @author zozlak
 */
class IndexerException extends Exception {

    const ERROR_DURING_IMPORT = 1;
    
    /**
     * A collection of already processed resources
     * @var array<RepoResource>
     */
    private $resources = [];

    /**
     * Creates the exception
     * @param string $message exception message
     * @param int $code exception code
     * @param Throwable|null $previous original exception
     * @param array<RepoResource> $resources collection of already commited resources
     */
    public function __construct(string $message = "", int $code = 0,
                                ?Throwable $previous = null,
                                array $resources = []) {
        parent::__construct($message, $code, $previous);
        $this->resources = $resources;
    }

    /**
     * Returns the collection of resources which were already commited when
     * an error occured.
     * @return array<RepoResource>
     */
    public function getCommitedResources(): array {
        return $this->resources;
    }
}
