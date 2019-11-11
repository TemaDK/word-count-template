package org.temadk.experiments;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.temadk.experiments.entities.Attribute;
import org.temadk.experiments.ml.NaiveBayesClassifier;
import org.temadk.experiments.services.FilesInformationService;

import javax.cache.Cache;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class NaiveBayes {
    private static final String MAILS = "Mails";
    private static final String ATTRIBUTES = "Attributes";

    public static void main(String[] args) throws Exception {
        FilesInformationService filesInformationService = new FilesInformationService(args[0]);
        String pathToData = args[1];

        int targetValue = Integer.parseInt(args[2]);
        int linesLimit = Integer.parseInt(args[3]);

        char separator = args[4].charAt(0);



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
                File dir = new File(pathToData);
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
                        parser = new CSVParser(new FileReader(f), CSVFormat.newFormat(separator));
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
                        for(Attribute a : filesInformationService.getAttributes())
                        {
                            if (data != "")
                                data += ",";
                            data += next.get((int)a.getOrder() - 1);
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
                    long availableVectors = cache.localMetrics().getCacheSize();

                    IgniteCache<Integer, String> cache2 = client.cache(ATTRIBUTES);
                    int i = 0;
                    while(cache2.get(i) != null){
                        String[] tokens = cache2.get(i).split(",");
                        attributes.add(new Attribute(Long.parseLong(tokens[0]),tokens[1],tokens[2]));
                        ++i;
                    }

                    NaiveBayesClassifier<String, String> localClassifier = new NaiveBayesClassifier<>();
                    int vectNum = 0;
                    for (Cache.Entry<Integer, String> entry : cache.localEntries()) {
                        vectNum++;
                        String data = entry.getValue();
                        String[] tokens = data.split(",");
                        if (tokens.length < attributes.size()) {
                            if (vectNum % 1000 == 0) updateProgress((float)vectNum/(float)availableVectors);
                            continue;
                        }
                        if (vectNum % 1000 == 0) updateProgress((float)vectNum/(float)availableVectors);
                        String age = tokens[targetValue - 1];
                        List<String> features = new ArrayList<>();
                        //получение данных по атрибутам
                        int j = 0;
                        for (Attribute a : attributes) {
                            String feature = a.getFieldName() + tokens[j++];
                            //System.out.println(feature + age);
                            features.add(feature);
                        }
                        //обучение локального классификатора
                        localClassifier.learn(age, features);
                    }
                    updateProgress(1);
                    System.out.println("Local training:" + (System.currentTimeMillis() - localTrainStart) / 1000 + "s");
                    System.out.println("Processed " + vectNum + " vectors");

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
            System.out.print("=");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.print("]");
    }
}
