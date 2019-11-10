package org.ivan.experiments;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.ivan.experiments.entities.Attribute;
import org.ivan.experiments.ml.NaiveBayesClassifier;
import org.ivan.experiments.services.FilesInformationService;

import javax.cache.Cache;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class NaiveBayes {
    private static final String MAILS = "Mails";
    private static final String ATTRIBUTES = "Attributes";
    private static final int targetValue = 18;

    private static int linesLimit = 8000000;
    private static FilesInformationService filesInformationService = new FilesInformationService();

    public static void main(String[] args) throws Exception {

        try (Ignite client = IgniteStarter.startClient()) {

            IgniteCache<Integer, String> cache = client.getOrCreateCache(new CacheConfiguration<>(MAILS));
            IgniteCache<Integer, String> cache2 = client.getOrCreateCache(new CacheConfiguration<>(ATTRIBUTES));

            cache.removeAll();
            cache2.removeAll();

            long parseStart = System.currentTimeMillis();
            System.out.println("Broadcasting...");

            //Load attributes
            try (IgniteDataStreamer<Integer, String> dataStreamer2 = client.dataStreamer(ATTRIBUTES)) {
                int i = 0;
                for (Attribute a : filesInformationService.getAttributes()) {
                    String data = a.getOrder()+","+a.getFieldName()+","+a.getDescription();
                    dataStreamer2.addData(i++, data);
                }
            }

            // Load data
            try (IgniteDataStreamer<Integer, String> dataStreamer = client.dataStreamer(MAILS)) {
                //поиск файлов с данным
                File dir = new File("data");
                File[] files = dir.listFiles();

                //ограничение на кол-во векторов
                boolean limit = false;
                int i = 0;
                //итерация для файла
                for (File f : files) {

                    long fileStart = System.currentTimeMillis();

                    CSVParser parser = null;
                    try {
                        //создание парсера для файла
                        parser = new CSVParser(new FileReader(f), CSVFormat.newFormat('|'));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    //создание итератора по файлу
                    Iterator<CSVRecord> iterator = parser.iterator();
                    iterator.next();
                    //итерация по файлу
                    while (iterator.hasNext()) {
                        String data = "";
                        CSVRecord next = iterator.next();
                        if(next.size() < filesInformationService.getAttributes().size()) continue;
                        for(int j = 0; j < filesInformationService.getAttributes().size(); ++j){
                            if (data != "")
                                data += ",";
                            data += next.get(j);
                        }
                        //Рассылка данных
                        dataStreamer.addData(i++, data);
                        //String tok[] = data.split(",");
                        //System.out.println("Data: " + data + " Length: " + tok.length);
                        //if (i % 10000 == 0) System.out.println(i);
                        if (i % 1000 == 0) updateProgress((float)i/(float)linesLimit);
                        if (i > linesLimit)
                        {
                            limit = true;
                            break;
                        }
                    }
                    if (i % 1000 == 0) updateProgress((float)i/(float)linesLimit);
                    if(limit){
                        System.out.println("File " + f.getName() + " parsed");
                        System.out.println("Number of entries: " + i);
                        System.out.println("Spent time: " + ((System.currentTimeMillis() - fileStart) / 1000 + "s"));
                        break;

                    }
                    System.out.println("File " + f.getName() + " parsed");
                    System.out.println("Number of entries: " + i);
                    System.out.println("Spent time: " + ((System.currentTimeMillis() - fileStart) / 1000 + "s"));

                }
            }

            IgniteCallable<NaiveBayesClassifier<String, String>> callable = new IgniteCallable<NaiveBayesClassifier<String, String>>() {
                @IgniteInstanceResource
                private Ignite localIgnite;
                //private String key = localIgnite.name() + new Date().toString();
                private List<Attribute> attributes = new ArrayList<Attribute>();
                @Override
                public NaiveBayesClassifier<String, String> call() throws Exception {
                    System.out.println("Begin to parsing and train local");
                    long localTrainStart = System.currentTimeMillis();

                    IgniteCache<Integer, String> cache = localIgnite.cache(MAILS);

                    IgniteCache<Integer, String> cache2 = client.cache(ATTRIBUTES);
                    int i = 0;
                    while(cache2.get(i) != null){
                        String[] tokens = cache2.get(i).split(",");
                        attributes.add(new Attribute(Long.parseLong(tokens[0]),tokens[1],tokens[2]));
                        ++i;
                    }

                    NaiveBayesClassifier<String, String> localClassifier = new NaiveBayesClassifier<>();

                    for (Cache.Entry<Integer, String> entry : cache.localEntries()) {

                        String data = entry.getValue();
                        String[] tokens = data.split(",");
                        if (tokens.length < attributes.size()) continue;
                        String age = tokens[targetValue];
                        List<String> features = new ArrayList<>();
                        //получение данных по атрибутам
                        for (Attribute a : attributes) {
                            String feature = a.getFieldName() + tokens[(int) a.getOrder() - 1];
                            //System.out.println(feature);
                            features.add(feature);
                        }
                        //обучение локального классификатора
                        localClassifier.learn(age, features);
                    }

                    System.out.println("Local training:");
                    System.out.println((System.currentTimeMillis() - localTrainStart) / 1000 + "s");
                    return localClassifier;
                }
            };
            System.out.println("All nodes full parsing time:" + (System.currentTimeMillis() - parseStart) / 1000 + "s");

            System.out.println("Training...");
            long startT = System.currentTimeMillis();
            Collection<NaiveBayesClassifier<String, String>> pieces = client.compute().broadcast(callable);
            //Создание общего классификатора
            NaiveBayesClassifier<String, String> main = new NaiveBayesClassifier<>();
            //объединение общего классификатора с другими классификаторами
            for(NaiveBayesClassifier<String, String> stringNaiveBayesClassifierEntry : pieces){
                main.merge(stringNaiveBayesClassifierEntry);
            }
            System.out.println("Reduce " + pieces.size() + " pieces");
            System.out.println("Full training time:" + (System.currentTimeMillis() - startT) / 1000 + "s");
        }
    }
    static void updateProgress(double progressPercentage) {
        final int width = 50;

        System.out.print("\r[");
        int i = 0;
        for (; i <= (int)(progressPercentage*width); i++) {
            System.out.print(".");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.print("]");
    }
}
