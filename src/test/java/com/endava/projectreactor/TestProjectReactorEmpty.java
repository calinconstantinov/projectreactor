package com.endava.projectreactor;

import net.datafaker.Faker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;

class TestProjectReactorEmpty {


    @Test
    void testFluxFromStream() {

    }


    @Test
    void testFluxCreate() {

    }


    @Test
    void testFluxDelay() throws InterruptedException {
        Flux.range(1, 10).delayElements(Duration.ofMillis(500))
            .subscribe(System.out::println);

        Thread.sleep(40000);
    }


    @Test
    void testFluxRandomDelay() {
        Flux.create(
            fluxSink -> {
                for (int i = 0; i < 10; i++) {
                    fluxSink.next(i);
                    try {
                        Thread.sleep(Faker.instance().random().nextInt(200, 2500));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        ).subscribe(System.out::println);
    }


    @Test
    void testFluxGenerate() {

    }


    @Test
    void testFluxMap() {

    }


    @Test
    void testSortAndDistinct() throws InterruptedException {
        Flux.range(1, 300)
            .map(integer -> Faker.instance().random().nextInt(1, 10))
            .delayElements(Duration.ofMillis(500))
            .sort()
            .distinct()
            .subscribe(System.out::println);

        Thread.sleep(100000);
    }


    @Test
    void testFluxToMonoSimple() {

    }


    @Test
    void testFluxToMonoComplex() {

    }


    @Test
    void testAllSubscribeMethods() {

    }


    @Test
    void testAllSubscribeMethodsError() {

    }


    @Test
    void testOnErrorContinue() {

    }


    @Test
    void testColdPublisher() throws InterruptedException {

    }


    @Test
    void testHotPublisher() throws InterruptedException {

    }


    @Test
    void testHotPublisherRestart() throws InterruptedException {

    }


    @Test
    void testHotHotPublisher() throws InterruptedException {

    }


    @Test
    void testMerge() throws InterruptedException {
      Flux<String> firstFlux =  Flux.range(1, 10)
            .map(i -> Faker.instance().name().firstName())
            .delayElements(Duration.ofMillis(1000));


        Flux<Integer> secondFlux =  Flux.range(1, 10)
            .map(i -> Faker.instance().random().nextInt(20, 50))
            .delayElements(Duration.ofMillis(2000));

        Flux.zip(firstFlux, secondFlux).subscribe(System.out::println);

        Thread.sleep(400000);

    }


    @Test
    void testZipNoBackpressure() throws InterruptedException {

    }


    @Test
    void testZipBackpressure() throws InterruptedException {

    }

}
