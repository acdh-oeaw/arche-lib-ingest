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

namespace acdhOeaw\arche\lib\ingest\tests;

use DateTime;
use GuzzleHttp\Exception\ClientException;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\RepoResourceInterface;
use acdhOeaw\arche\lib\exception\Deleted;
use acdhOeaw\arche\lib\exception\NotFound;

/**
 * Description of HelpersTrait
 *
 * @author zozlak
 */
abstract class TestBase extends \PHPUnit\Framework\TestCase {

    static protected Repo $repo;
    static protected object $config;
    static protected string $test = '';

    static public function setUpBeforeClass(): void {
        $cfgFile      = __DIR__ . '/config.yaml';
        self::$config = json_decode(json_encode(yaml_parse_file($cfgFile)));
        self::$repo   = Repo::factory($cfgFile);
        if (file_exists(__DIR__ . '/time.log')) {
            unlink(__DIR__ . '/time.log');
        }
    }

    static public function tearDownAfterClass(): void {
        echo "\n" . file_get_contents(__DIR__ . '/time.log');
    }

    /**
     * 
     * @var array<RepoResource>
     */
    private array $resources;
    private float $time = 0;

    public function setUp(): void {
        $this->resources = [];
        $this->startTimer();
    }

    public function tearDown(): void {
        $this->noteTime(static::class . "::" . self::$test . "()");
        self::$repo->rollback();

        self::$repo->begin();
        foreach ($this->resources as $r) {
            try {
                if ($r instanceof RepoResourceInterface) {
                    $r->delete(true, true, self::$config->schema->parent);
                }
            } catch (Deleted $e) {
                
            } catch (NotFound $e) {
                
            }
        }
        self::$repo->commit();
        if (is_dir(__DIR__ . '/tmp')) {
            system('rm -fR ' . __DIR__ . '/tmp');
        }
    }

    /**
     * 
     * @param array<RepoResource|ClientException> $res
     * @return void
     */
    protected function noteResources(array $res): void {
        $this->resources = array_merge($this->resources, array_values($res));
    }

    protected function startTimer(): void {
        $this->time = microtime(true);
    }

    protected function noteTime(string $msg = ''): void {
        $t = microtime(true) - $this->time;
        $t = sprintf("%.6f", $t);
        file_put_contents(__DIR__ . '/time.log', "$t\t$msg\n", \FILE_APPEND);
    }
}
