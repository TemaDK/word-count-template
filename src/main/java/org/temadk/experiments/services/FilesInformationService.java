package org.temadk.experiments.services;

import org.temadk.experiments.entities.Attribute;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FilesInformationService {

    //список атрибутов
    public List<Attribute> attributes;

    //конструктор класса
    public FilesInformationService(String  path) {
        //String attributes_dir = "attributes\\attributes.csv";
        //String attributes_dir = "attributes\\attributes.csv";
        File attributes_file = new File(path);

        CSVParser parser = null;
        try {
            //создание парсера для файла
            parser = new CSVParser(new FileReader(attributes_file), CSVFormat.newFormat(','));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int i = 0;

        attributes = new ArrayList<Attribute>();

        //создание итератора по файлу
        Iterator<CSVRecord> iterator = parser.iterator();
        iterator.next();
        ++i;
        //итерация по файлу
        while (iterator.hasNext()) {
            CSVRecord next = iterator.next();
            attributes.add(new Attribute(Long.parseLong(next.get(0)), next.get(1), next.get(2)));
            ++i;
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

}
