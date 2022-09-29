package com.endava.projectreactor;

import net.datafaker.Faker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

        flux.log().subscribe(
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

        flux.subscribe(
            o -> System.out.println("Received: " + o),
            e -> System.out.println("Error: " + e.getMessage()),
            () -> System.out.println("Completed!"));
    }


    @Test
    void testDefaultIfEmpty() {
        Flux.just(1, 2, 3)
            .filter(i -> i == 4)
            .defaultIfEmpty(100)
            .subscribe(i -> System.out.println("Received: " + i));
    }


    @Test
    void testSwitchIfEmpty() {
        Flux.just(1, 2, 3)
            .filter(i -> i == 4)
            .switchIfEmpty(Flux.range(3, 6))
            .subscribe(i -> System.out.println("Received: " + i));
    }


    @Test
    void testSwitchOnFirst() {
        Flux.just(1, 2, 3, 4, 5)
            .map(i -> Faker.instance().random().nextInt(1, 10))
            .switchOnFirst(
                (signal, integerFlux) -> {
                    if (signal.isOnNext() && signal.get() % 2 == 0) {
                        return integerFlux;
                    }
                    return Flux.just(20, 20, 20);
                }
            )
            .switchIfEmpty(Flux.range(3, 6))
            .subscribe(i -> System.out.println("Received: " + i));
    }


    @Test
    void testRetry() {
        Flux.range(1, 10)
            .flatMap(integer -> {
                int value = Faker.instance().random().nextInt(1, 10);
                System.out.println("Created: " + value);
                if (value != 5) {
                    return Mono.error(new RuntimeException("error!"));
                }
                return Mono.just(value);
            })
            .next()
            .retry()
            .subscribe(i -> System.out.println("Received: " + i));
    }


    @Test
    void testColdPublisher() throws InterruptedException {
        Flux<String> coldStream = Flux.fromIterable(
                List.of("Person 1", "Person 2", "Person 3", "Person 4", "Person 5"))
            .delayElements(Duration.ofMillis(1500));

        coldStream.subscribe(s -> System.out.println("Subscriber 1: " + s));

        Thread.sleep(3100);

        coldStream.subscribe(s -> System.out.println("Subscriber 2: " + s));

        Thread.sleep(20000);
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

        Thread.sleep(20000);
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

        Thread.sleep(20000);
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

        Thread.sleep(20000);
    }


    @Test
    void testMerge() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofSeconds(1))
            .map(aLong -> Faker.instance().name().fullName());

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.merge(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-25s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(20000);
    }


    @Test
    void testZipNoBackpressure() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofMillis(400))
            .map(aLong -> Faker.instance().name().fullName());

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.zip(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-25s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(20000);
    }


    @Test
    void testZipBackpressure() throws InterruptedException {
        Flux<String> alternate1 = Flux.interval(Duration.ofSeconds(400))
            .map(aLong -> Faker.instance().name().fullName()).onBackpressureBuffer();

        Flux<Integer> alternate2 = Flux.interval(Duration.ofSeconds(4))
            .map(aLong -> Faker.instance().random().nextInt(10, 30));

        Flux.zip(alternate1, alternate2).subscribe(data ->
            System.out.printf("%-25s --- %5s%n", data, Thread.currentThread().getName()));

        System.out.println("Thread : " + Thread.currentThread().getName());
        Thread.sleep(20000);
    }


    @Test
    void testSubscribeOn() throws InterruptedException {
        Flux.generate(synchronousSink -> {
                int element = Faker.instance().random().nextInt(1, 20);
                System.out.printf("%-15s --- %15s%n", "Created: " + element, Thread.currentThread().getName());
                synchronousSink.next(element);
                if (element == 5) {
                    synchronousSink.complete();
                }
            })
            .subscribeOn(Schedulers.boundedElastic())
            .doFirst(() -> System.out.printf("%-15s --- %15s%n", "Assembly time!", Thread.currentThread().getName()))
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testSubscribeOnSubscribeOn() throws InterruptedException {
        Flux<Integer> flux = Flux.<Integer>create(fluxSink -> {
                for (int i = 0; i < 3; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "First next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.parallel())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Second next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.boundedElastic());

        flux.subscribe(
            data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testSubscribeOnBoundedElasticMultipleSubscribers() throws InterruptedException {
        Flux<Integer> flux = Flux.<Integer>create(fluxSink -> {
                for (int i = 0; i < 3; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .subscribeOn(Schedulers.boundedElastic());

        for (int i = 0; i < 4; i++) {
            final int subscriberNumber = i;
            flux.subscribe(
                data -> System.out.printf("%-15s --- %15s%n",
                    subscriberNumber + " Received: " + data, Thread.currentThread().getName()));
        }

        Thread.sleep(3000);
    }


    @Test
    void testPublishOn() throws InterruptedException {
        Flux<Integer> flux = Flux.create(fluxSink -> {
            for (int i = 0; i < 3; i++) {
                System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                fluxSink.next(i);
            }
            fluxSink.complete();
        });

        flux
            .publishOn(Schedulers.parallel())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testPublishOnPublishOn() throws InterruptedException {
        Flux<Integer> flux = Flux.create(fluxSink -> {
            for (int i = 0; i < 3; i++) {
                System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                fluxSink.next(i);
            }
            fluxSink.complete();
        });

        flux = flux
            .publishOn(Schedulers.parallel())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Next: " + data,
                    Thread.currentThread().getName()));

        flux.publishOn(Schedulers.boundedElastic())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));


        Thread.sleep(3000);
    }


    @Test
    void testSubscribeOnPublishOn() throws InterruptedException {
        Flux.create(fluxSink -> {
                for (int i = 0; i < 3; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "First next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.parallel())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Second next: " + data, Thread.currentThread().getName()))
            .publishOn(Schedulers.boundedElastic())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testPublishOnSubscribeOn() throws InterruptedException {
        Flux.create(fluxSink -> {
                for (int i = 0; i < 3; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "First next: " + data, Thread.currentThread().getName()))
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Second next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.parallel())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testSubscribeOnSubscribeOnPublishOnPublishOnMultipleSubscribers() throws InterruptedException {
        Flux<Integer> flux = Flux.<Integer>create(fluxSink -> {
                for (int i = 0; i < 3; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Top next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.newParallel("topParallel"))
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Middle next: " + data, Thread.currentThread().getName()))
            .subscribeOn(Schedulers.boundedElastic());

        flux = flux
            .publishOn(Schedulers.parallel())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Bottom next: " + data,
                    Thread.currentThread().getName()));


        Scheduler newParallel = Schedulers.newParallel("bottomParallel");

        flux.publishOn(newParallel)
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        flux.publishOn(newParallel)
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testRunOn() throws InterruptedException {
        Flux.create(fluxSink -> {
                for (int i = 0; i < 10; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .parallel()
            .runOn(Schedulers.boundedElastic())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }


    @Test
    void testSequential() throws InterruptedException {
        Flux.create(fluxSink -> {
                for (int i = 0; i < 10; i++) {
                    System.out.printf("%-15s --- %15s%n", "Created: " + i, Thread.currentThread().getName());
                    fluxSink.next(i);
                }
                fluxSink.complete();
            })
            .parallel()
            .runOn(Schedulers.boundedElastic())
            .doOnNext(
                data -> System.out.printf("%-15s --- %15s%n", "Next: " + data, Thread.currentThread().getName()))
            .sequential()
            .publishOn(Schedulers.parallel())
            .subscribe(
                data -> System.out.printf("%-15s --- %15s%n", "Received: " + data, Thread.currentThread().getName()));

        Thread.sleep(3000);
    }

}
