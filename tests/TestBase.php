<?php

/*
 * The MIT License
 *
 * Copyright 2019 zozlak.
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

use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\exception\Deleted;
use acdhOeaw\acdhRepoLib\exception\NotFound;

/**
 * Description of HelpersTrait
 *
 * @author zozlak
 */
abstract class TestBase extends \PHPUnit\Framework\TestCase {

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    static protected $repo;
    static protected $config;

    static public function setUpBeforeClass(): void {
        $cfgFile      = __DIR__ . '/../../rdbms/config.yaml';
        self::$config = json_decode(json_encode(yaml_parse_file($cfgFile)));
        self::$repo   = Repo::factory($cfgFile);
    }

    static public function tearDownAfterClass(): void {
        
    }

    private $resources;

    public function setUp(): void {
        $this->resources = [];
    }

    public function tearDown(): void {
        self::$repo->rollback();
        self::$repo->begin();
        foreach ($this->resources as $i) {
            /* @var $i \acdhOeaw\acdhRepoLib\RepoResource */
            try {
                $i->delete(true, true);
            } catch (Deleted $e) {
                
            } catch (NotFound $e) {
                
            }
        }
        self::$repo->commit();
        if (is_dir(__DIR__ . '/tmp')) {
            system('rm -fR ' . __DIR__ . '/tmp');
        }
    }

    protected function noteResources(array $res): void {
        $this->resources = array_merge($this->resources, $res);
    }
}
