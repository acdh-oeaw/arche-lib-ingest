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

/**
 * Description of MetadataCollectionTest
 *
 * @author zozlak
 */
class MetadataCollectionTest extends TestBase {
/*
    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        //MetadataCollection::$debug = true;
    }

    public function testSimple(): void {
        $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $this->resources = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        self::$repo->commit();

        $this->assertEquals(4, count($this->resources));
    }

    public function testSimpleDouble(): void {
        $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $resources1      = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->resources = $resources1;
        self::$repo->commit();

        $this->assertEquals(4, count($resources1));

        $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-small.ttl');
        self::$repo->begin();
        $resources2      = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        $this->resources = array_merge($this->resources, $resources2);
        self::$repo->commit();

        $this->assertEquals(4, count($resources2));
        $r1u = $r2u = [];
        foreach ($resources1 as $i) {
            $r1u[] = $i->getUri();
        }
        foreach ($resources2 as $i) {
            $r2u[] = $i->getUri();
        }
        $this->assertEquals(count($r1u), count($r2u));
        $this->assertEquals([], array_diff($r2u, $r1u));
    }

    public function testLarge(): void {
        $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-large.ttl');
        self::$repo->begin();
        $this->resources = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
        self::$repo->commit();

        $this->assertEquals(19, count($this->resources));
    }

    public function testCycle(): void {
        $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/graph-cycle.ttl');
        self::$repo->begin();
        $this->resources = $graph->import('http://some.id', MetadataCollection::SKIP);
        self::$repo->commit();

        $this->assertEquals(2, count($this->resources));
    }
*/
    /**
     * @large
     */
    /*
      public function testBig(): void {
      $graph           = new MetadataCollection(self::$repo, __DIR__ . '/data/schnitzler-diaries.rdf');
      self::$repo->begin();
      $this->resources = $graph->import('https://id.acdh.oeaw.ac.at/', MetadataCollection::SKIP);
      self::$repo->commit();

      $this->assertEquals(75, count($this->resources));
      }
     */
}
