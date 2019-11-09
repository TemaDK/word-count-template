package org.ivan.experiments.ml;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
реализация алгоритма Naive Bayes
 */
public class NaiveBayesClassifier<T, K> extends Classifier<T, K> implements Serializable {

    private static final long serialVersionUID = 5504911666956811967L;

    //базовый конструткор класса
    public NaiveBayesClassifier() {
    }

    //конструктор на базе имеющегося классификатора
    public NaiveBayesClassifier(NaiveBayesClassifier classifier) {
        merge(classifier);
    }

    //метод для слияния с другим классификатором
    public void merge(NaiveBayesClassifier classifier) {
        Hashtable<K, Hashtable<T, Integer>> table = new Hashtable<>(this.featureCountPerCategory);
        for (Map.Entry<K, Hashtable<T, Integer>> entry : table.entrySet()) {
            if (classifier.featureCountPerCategory.get(entry.getKey()) != null) {
                table.put(entry.getKey(), mergeMaps(entry.getValue(),
                        (Hashtable<T, Integer>) classifier.featureCountPerCategory.get(entry.getKey())));
            }
        }
        Set<Map.Entry<K, Hashtable<T, Integer>>> set = classifier.featureCountPerCategory.entrySet();
        for (Map.Entry<K, Hashtable<T, Integer>> entry : set) {
            if (table.get(entry.getKey()) == null) {
                table.put(entry.getKey(), entry.getValue());
            }
        }
        this.featureCountPerCategory = table;
        this.totalCategoryCount = mergeMaps((Map<K, Integer>) this.totalCategoryCount,
                classifier.totalCategoryCount);
        this.totalFeatureCount = mergeMaps((Map<T, Integer>) this.totalFeatureCount,
                classifier.totalFeatureCount);
        this.memoryQueue.addAll(classifier.memoryQueue);
    }

    //метод для слияния двух ассоциативных массивов
    private Hashtable mergeMaps(Map<? extends Object, Integer> m1, Map<? extends Object, Integer> m2) {
        Hashtable map = new Hashtable<>(
                Stream.of(m1, m2)
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        Integer::sum
                                )
                        ));
        return map;
    }

    //расчет вероятности пересечения нескольких признаков и категории
    private float featuresProbabilityProduct(Collection<T> features,
                                             K category) {
        float product = 1.0f;
        for (T feature : features)
            product *= this.featureWeighedAverage(feature, category);
        return product;
    }

    //расчет вероятности категории на базе признаков
    private float categoryProbability(Collection<T> features, K category) {
        return ((float) this.getCategoryCount(category)
                / (float) this.getCategoriesTotal())
                * featuresProbabilityProduct(features, category);
    }

    //расчет вероятности нескольких признаков
    private SortedSet<Classification<T, K>> categoryProbabilities(Collection<T> features) {


        SortedSet<Classification<T, K>> probabilities =
                new TreeSet<Classification<T, K>>(
                        new Comparator<Classification<T, K>>() {

                            public int compare(Classification<T, K> o1,
                                               Classification<T, K> o2) {
                                int toReturn = Float.compare(
                                        o1.getProbability(), o2.getProbability());
                                if ((toReturn == 0)
                                        && !o1.getCategory().equals(o2.getCategory()))
                                    toReturn = -1;
                                return toReturn;
                            }
                        });

        for (K category : this.getCategories())
            probabilities.add(new Classification<T, K>(
                    features, category,
                    this.categoryProbability(features, category)));
        return probabilities;
    }

    //классификация признаков
    @Override
    public Classification<T, K> classify(Collection<T> features) {
        SortedSet<Classification<T, K>> probabilites =
                this.categoryProbabilities(features);

        if (probabilites.size() > 0) {
            return probabilites.last();
        }
        return null;
    }

    //детализированная классификация признаков
    public Collection<Classification<T, K>> classifyDetailed(
            Collection<T> features) {
        return this.categoryProbabilities(features);
    }
}

