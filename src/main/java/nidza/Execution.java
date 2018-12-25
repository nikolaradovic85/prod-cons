package nidza;

import org.asynchttpclient.*;
import org.json.JSONObject;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class UrlConsumer implements Runnable {

  BlockingQueue<Integer> urlsQueue;
  BlockingQueue<String> responsesQueue;
  AtomicInteger counter;

  public UrlConsumer(BlockingQueue<Integer> queue, BlockingQueue<String> responseQueue, AtomicInteger counter) {
    this.urlsQueue = queue;
    this.responsesQueue = responseQueue;
    this.counter = counter;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Integer element = urlsQueue.take();
        int counterLocal = counter.getAndIncrement();
        System.out.println("Processing " + counterLocal + " on thread " + Thread.currentThread().getName());
        System.out.println("Processing url: " + element + " on thread " + Thread.currentThread().getName());
        DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config()
            .setConnectTimeout(500)
            .setThreadPoolName("NidzaConnections")
            .setMaxConnections(10);
        try (AsyncHttpClient client = Dsl.asyncHttpClient(clientBuilder)) {
          client
              .prepareGet("http://172.17.0.2/get")
              .addQueryParam("element", element + "-" + counterLocal)
              .addQueryParam("thread", Thread.currentThread().getName())
              .execute()
              .toCompletableFuture()
              .whenComplete(
                  (response, throwable) ->
                  {
                    String elementData = new JSONObject(response.getResponseBody()).getJSONObject("args").getString("element");
                    String threadData = new JSONObject(response.getResponseBody()).getJSONObject("args").getString("thread");
                    System.out.println("Got response: " + elementData + " on thread " + Thread.currentThread().getName());
                    try {
                      responsesQueue.put("Element: " + elementData + " / thread " + threadData + " / async " + Thread.currentThread().getName());
                    } catch (InterruptedException e) {
                      System.out.println("Stopping thread " + Thread.currentThread().getName());
                      e.printStackTrace();
                    }
                  }
              ).join();
        }
      } catch (InterruptedException e) {
        System.out.println("Stopping thread " + Thread.currentThread().getName());
        return;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}

class OutputConsumer implements Runnable {
  BlockingQueue<String> outputs;

  public OutputConsumer(BlockingQueue<String> outputs) {
    this.outputs = outputs;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try (FileOutputStream loggerStream = new FileOutputStream("output", true)) {
        if (outputs.remainingCapacity() > 0) {
          String element = outputs.take();
          System.out.println("printing element: " + element + " on thread " + Thread.currentThread().getName());
          loggerStream.write(element.getBytes());
          loggerStream.write(System.lineSeparator().getBytes());
        }
      }  catch (IOException e){
        e.printStackTrace();
      } catch (InterruptedException e) {
        System.out.println("Stopping thread " + Thread.currentThread().getName());
        return;
      }
    }
  }
}


public class Execution {

  public static void main(String[] args) throws Exception {
    BlockingQueue<String> outputQueue = new ArrayBlockingQueue<>(100);

    Thread logger = new Thread(new OutputConsumer(outputQueue), "Logger");
    logger.start();

    BlockingQueue<Integer> urlQueue = new ArrayBlockingQueue<>(10);
    AtomicInteger counter = new AtomicInteger(0);
    final List<Runnable> urlConsumers = Arrays.asList(
        new UrlConsumer(urlQueue, outputQueue, counter),
        new UrlConsumer(urlQueue, outputQueue, counter),
        new UrlConsumer(urlQueue, outputQueue, counter)
    );

    List<Thread> urlRunningThreads = urlConsumers.stream().map(runnable -> new Thread(runnable)).peek(Thread::start).collect(Collectors.toList());

    try (InputStream fileInput = new FileInputStream("src/main/resources/urls")) {

      Scanner scanner = new Scanner(fileInput);

      while (scanner.hasNextLine()) {
        urlQueue.put(Integer.parseInt(scanner.nextLine()));
      }
    }
    TimeUnit.SECONDS.sleep(1);
    urlRunningThreads.forEach(Thread::interrupt);
    logger.interrupt();
    System.out.println("Exiting ...");
  }
}