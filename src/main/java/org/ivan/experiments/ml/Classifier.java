package org.ivan.experiments.ml;

import java.io.Serializable;
import java.util.*;


public abstract class Classifier<T, K> implements FeatureProbability<T, K>, Serializable {


    //уникальный номер для сериализации
    private static final long serialVersionUID = 5504911666956811966L;

    //начальнай размер словаря
    private static final int INITIAL_CATEGORY_DICTIONARY_CAPACITY = 16;

    //начальный размер словаря
    private static final int INITIAL_FEATURE_DICTIONARY_CAPACITY = 32;

    //объем памяти
    private int memoryCapacity = 1000;

    //таблица пересечений классов и признаков
    protected Hashtable<K, Hashtable<T, Integer>> featureCountPerCategory;

    //таблица для количества каждого признака
    protected Hashtable<T, Integer> totalFeatureCount;

    //таблица для количества каждого класса
    protected Hashtable<K, Integer> totalCategoryCount;

    //очередь для последних изменений
    protected Queue<Classification<T, K>> memoryQueue;

    //базовый конструктор
    public Classifier() {
        this.reset();
    }

    //перезагрузка классификатора
    public void reset() {
        this.featureCountPerCategory = new Hashtable<K, Hashtable<T, Integer>>(
                Classifier.INITIAL_CATEGORY_DICTIONARY_CAPACITY);
        this.totalFeatureCount = new Hashtable<T, Integer>(Classifier.INITIAL_FEATURE_DICTIONARY_CAPACITY);
        this.totalCategoryCount = new Hashtable<K, Integer>(Classifier.INITIAL_CATEGORY_DICTIONARY_CAPACITY);
        this.memoryQueue = new LinkedList<Classification<T, K>>();
    }

    /*
    методы get и set для получения и изменения полей класса
     */
    public Set<T> getFeatures() {
        return ((Hashtable<T, Integer>) this.totalFeatureCount).keySet();
    }


    public Set<K> getCategories() {
        return ((Hashtable<K, Integer>) this.totalCategoryCount).keySet();
    }


    public int getCategoriesTotal() {
        int toReturn = 0;
        for (Enumeration<Integer> e = this.totalCategoryCount.elements(); e.hasMoreElements(); ) {
            toReturn += e.nextElement();
        }
        return toReturn;
    }


    public int getMemoryCapacity() {
        return memoryCapacity;
    }


    public void setMemoryCapacity(int memoryCapacity) {
        for (int i = this.memoryCapacity; i > memoryCapacity; i--) {
            this.memoryQueue.poll();
        }
        this.memoryCapacity = memoryCapacity;
    }

    //увеличение частоты признака для категории
    public void incrementFeature(T feature, K category) {
        Dictionary<T, Integer> features = this.featureCountPerCategory.get(category);
        if (features == null) {
            this.featureCountPerCategory.put(category,
                    new Hashtable<T, Integer>(Classifier.INITIAL_FEATURE_DICTIONARY_CAPACITY));
            features = this.featureCountPerCategory.get(category);
        }
        Integer count = features.get(feature);
        if (count == null) {
            features.put(feature, 0);
            count = features.get(feature);
        }
        features.put(feature, ++count);

        Integer totalCount = this.totalFeatureCount.get(feature);
        if (totalCount == null) {
            this.totalFeatureCount.put(feature, 0);
            totalCount = this.totalFeatureCount.get(feature);
        }
        this.totalFeatureCount.put(feature, ++totalCount);
    }

    //увеличение частоты категории
    public void incrementCategory(K category) {
        Integer count = this.totalCategoryCount.get(category);
        if (count == null) {
            this.totalCategoryCount.put(category, 0);
            count = this.totalCategoryCount.get(category);
        }
        this.totalCategoryCount.put(category, ++count);
    }

    //уменьшение частоты признака для категории
    public void decrementFeature(T feature, K category) {
        Dictionary<T, Integer> features = this.featureCountPerCategory.get(category);
        if (features == null) {
            return;
        }
        Integer count = features.get(feature);
        if (count == null) {
            return;
        }
        if (count.intValue() == 1) {
            features.remove(feature);
            if (features.size() == 0) {
                this.featureCountPerCategory.remove(category);
            }
        } else {
            features.put(feature, --count);
        }

        Integer totalCount = this.totalFeatureCount.get(feature);
        if (totalCount == null) {
            return;
        }
        if (totalCount.intValue() == 1) {
            this.totalFeatureCount.remove(feature);
        } else {
            this.totalFeatureCount.put(feature, --totalCount);
        }
    }

    //уменьшение частоты категории
    public void decrementCategory(K category) {
        Integer count = this.totalCategoryCount.get(category);
        if (count == null) {
            return;
        }
        if (count.intValue() == 1) {
            this.totalCategoryCount.remove(category);
        } else {
            this.totalCategoryCount.put(category, --count);
        }
    }

    /*
    методы get для получения полей класса
     */
    public int getFeatureCount(T feature, K category) {
        Dictionary<T, Integer> features = this.featureCountPerCategory.get(category);
        if (features == null) return 0;
        Integer count = features.get(feature);
        return (count == null) ? 0 : count.intValue();
    }


    public int getFeatureCount(T feature) {
        Integer count = this.totalFeatureCount.get(feature);
        return (count == null) ? 0 : count.intValue();
    }


    public int getCategoryCount(K category) {
        Integer count = this.totalCategoryCount.get(category);
        return (count == null) ? 0 : count.intValue();
    }

    //расчет вероятности признака для заданной категории
    public float featureProbability(T feature, K category) {
        final float totalFeatureCount = this.getFeatureCount(feature);

        if (totalFeatureCount == 0) {
            return 0;
        } else {
            return this.getFeatureCount(feature, category) / (float) this.getFeatureCount(feature);
        }
    }

    //взвешенный расчет вероятности признака для заданной категории
    public float featureWeighedAverage(T feature, K category) {
        return this.featureWeighedAverage(feature, category, null, 1.0f, 0.5f);
    }

    //взвешенный расчет вероятности признака для заданной категории с заданным калькульлятором
    public float featureWeighedAverage(T feature, K category, FeatureProbability<T, K> calculator) {
        return this.featureWeighedAverage(feature, category, calculator, 1.0f, 0.5f);
    }

    //взвешенный расчет вероятности признака для заданной категории с заданным калькульлятором
    public float featureWeighedAverage(T feature, K category, FeatureProbability<T, K> calculator, float weight) {
        return this.featureWeighedAverage(feature, category, calculator, weight, 0.5f);
    }


    //взвешенный расчет вероятности признака для заданной категории с заданным калькульлятором
    public float featureWeighedAverage(T feature, K category, FeatureProbability<T, K> calculator, float weight,
                                       float assumedProbability) {

        /*
         * use the given calculating object or the default method to calculate
         * the probability that the given feature occurred in the given
         * category.
         */
        final float basicProbability = (calculator == null) ? this.featureProbability(feature, category)
                : calculator.featureProbability(feature, category);

        Integer totals = this.totalFeatureCount.get(feature);
        if (totals == null) totals = 0;
        return (weight * assumedProbability + totals * basicProbability) / (weight + totals);
    }

    //метод обучения классификтора с помощью категории и заданных признаков
    public void learn(K category, Collection<T> features) {
        this.learn(new Classification<T, K>(features, category));
    }

    //метод обучения классификатора с помощью заданной классификации
    public void learn(Classification<T, K> classification) {

        for (T feature : classification.getFeatureset())
            this.incrementFeature(feature, classification.getCategory());
        this.incrementCategory(classification.getCategory());

        this.memoryQueue.offer(classification);
        if (this.memoryQueue.size() > this.memoryCapacity) {
            Classification<T, K> toForget = this.memoryQueue.remove();

            for (T feature : toForget.getFeatureset())
                this.decrementFeature(feature, toForget.getCategory());
            this.decrementCategory(toForget.getCategory());
        }
    }

    //абстрактный метод классификации на основе заданных атрибутов
    public abstract Classification<T, K> classify(Collection<T> features);
}

