package com.endava.projectreactor;

import net.datafaker.Faker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

class TestProjectReactor {

    @Test
    void testFluxFromArray() {
        Flux<String> flux = Flux.fromArray(new String[]{"a", "b", "c"});
        flux.subscribe(System.out::println);
    }


    @Test
    void testFluxFromStream() {
        Flux<String> flux = Flux.fromStream(Stream.of("a", "b", "c"));
        flux.subscribe(System.out::println);
    }


    @Test
    void testFluxCreate() {
        Flux<Object> flux = Flux.create(fluxSink -> {
            System.out.println("created");
            for (int i = 0; i < 5; i++) {
                fluxSink.next(i);
            }
            fluxSink.complete();
        });

        flux.subscribe(System.out::println);
    }


    @Test
    void testAllSubscribeMethods() {
        Flux<Object> flux = Flux.just(1, 2, 3, "a", Faker.instance().name().fullName());

        flux.subscribe(
            o -> System.out.println("Received: " + o),
            e -> System.out.println("Error: " + e.getMessage()),
            () -> System.out.println("Completed!"));
    }


    @Test
    void testFluxDelay() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1, 10)
            .delayElements(Duration.ofMillis(500));

        flux.subscribe(System.out::println);

        Thread.sleep(8000);
    }


    @Test
    void testFluxRandomDelay() {
        Flux<Object> flux = Flux.create(fluxSink -> {
            System.out.println("Created Flux!");

            for (int i = 0; i < 5; i++) {
                fluxSink.next(Faker.instance().name().name());
                System.out.println("Created new element!");

                try {
                    Thread.sleep(Faker.instance().random().nextInt(500, 2500));
                } catch (InterruptedException e) {
                    fluxSink.error(new RuntimeException(e));
                    return;
                }
            }
            fluxSink.complete();
        });


        flux.take(2).subscribe(
            o -> System.out.println("Received: " + o),
            e -> System.out.println("Error: " + e.getMessage()),
            () -> System.out.println("Completed!"));
    }


    @Test
    void testFluxGenerate() {
        Flux.generate(synchronousSink -> {
                System.out.println("Created new element!");
                synchronousSink.next(Faker.instance().name().name());
                if (Faker.instance().random().nextInt(1, 10) == 5) {
                    synchronousSink.complete();
                }

                try {
                    Thread.sleep(Faker.instance().random().nextInt(500, 2500));
                } catch (InterruptedException e) {
                    synchronousSink.error(new RuntimeException(e));
                }
            })
            .take(2)
            .subscribe(o -> System.out.println("Received: " + o));
    }


    @Test
    void testFluxMap() {
        Flux<String> range = Flux.range(1, 9)
            .map(i -> Faker.instance().name().fullName());
        range.subscribe(System.out::println);
    }


    @Test
    void testSortAndDistinct() {
        Flux.range(1, 1000).map(integer -> Faker.instance().random().nextInt(1, 25))
            .sort()
            .distinct()
            .subscribe(System.out::println);
    }


    @Test
    void testFluxToMonoSimple() {
        Flux.range(1, 10)
            .next()
            .subscribe(System.out::println);
    }


    @Test
    void testFluxToMonoComplex() {
        Flux.range(1, 10)
            .filter(i -> i > 5)
            .take(2)
            .next()
            .subscribe(System.out::println);
    }


    @Test
    void testError() {
        Flux<Object> flux = Flux.just(1, 2, 3, "a", Faker.instance().name().fullName())
            .map(v -> (Integer) v);

        flux.subscribe(
            o -> System.out.println("Received: " + o),
            e -> System.out.println("Error: " + e.getMessage()),
            () -> System.out.println("Completed!"));
    }


    @Test
    void testOnErrorContinue() {
        Flux<Integer> flux = Flux.just(1, 2, 3, "a", Faker.instance().name().fullName())
            .map(v -> (Integer) v)
            .onErrorContinue((throwable, o) -> {
                System.out.println("Some error encountered:" + throwable.getMessage());
            });

        flux.log().subscribe(
            o -> System.out.println("Received: " + o),
            e -> System.out.println("Error: " + e.getMessage()),
            () -> System.out.println("Completed!"));
    }


    @Test
    void testColdPublisher() throws InterruptedException {
        Flux<String> coldStream = Flux.fromIterable(
                List.of("Person 1", "Person 2", "Person 3", "Person 4", "Person 5"))
            .delayElements(Duration.ofMillis(1500));

        coldStream.subscribe(s -> System.out.println("Subscriber 1: " + s));

        Thread.sleep(3100);

        coldStream.subscribe(s -> System.out.println("Subscriber 2: " + s));

        Thread.sleep(30000);
    }


    @Test
    void testHotPublisher() throws InterruptedException {
        Flux<String> coldStream = Flux.fromIterable(
                List.of("Person 1", "Person 2", "Person 3", "Person 4", "Person 5"))
            .delayElements(Duration.ofMillis(1500)).share();

        Thread.sleep(1600);

        coldStream.subscribe(s -> System.out.println("Subscriber 1: " + s));

        Thread.sleep(3100);

        coldStream.subscribe(s -> System.out.println("Subscriber 2: " + s));

        Thread.sleep(30000);
    }


    @Test
    void testHotPublisherRestart() throws InterruptedException {
        Flux<String> coldStream = Flux.fromIterable(
                List.of("Person 1", "Person 2", "Person 3", "Person 4", "Person 5"))
            .delayElements(Duration.ofMillis(1500)).publish().refCount(1);

        Thread.sleep(1600);

        coldStream.subscribe(s -> System.out.println("Subscriber 1: " + s));

        Thread.sleep(3100);

        coldStream.subscribe(s -> System.out.println("Subscriber 2: " + s));

        Thread.sleep(6100);

        coldStream.subscribe(s -> System.out.println("Subscriber 3: " + s));

        Thread.sleep(30000);
    }


    @Test
    void testHotHotPublisher() throws InterruptedException {
        Flux<String> coldStream = Flux.fromIterable(
                List.of("Person 1", "Person 2", "Person 3", "Person 4", "Person 5"))
            .delayElements(Duration.ofMillis(1500)).publish().autoConnect(0);

        Thread.sleep(1600);

        coldStream.subscribe(s -> System.out.println("Subscriber 1: " + s));

        Thread.sleep(3100);

        coldStream.subscribe(s -> System.out.println("Subscriber 2: " + s));

        Thread.sleep(6100);

        coldStream.subscribe(s -> System.out.println("Subscriber 3: " + s));

        Thread.sleep(30000);
    }


    @Test
    void testMerge() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofSeconds(1))
            .map(aLong -> Faker.instance().name().fullName());

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.merge(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-20s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(100000);
    }


    @Test
    void testZipNoBackpressure() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofSeconds(1))
            .map(aLong -> Faker.instance().name().fullName());

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.zip(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-25s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(100000);
    }


    @Test
    void testZipBackpressure() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofSeconds(1))
            .map(aLong -> Faker.instance().name().fullName()).onBackpressureBuffer();

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.zip(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-25s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(100000);
    }

}
