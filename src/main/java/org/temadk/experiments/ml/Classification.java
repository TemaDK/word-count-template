package org.temadk.experiments.ml;

import java.io.Serializable;
import java.util.Collection;

/*
Класс классификации
 */
public class Classification<T, K> implements Serializable {

    //номер для сериализации
    private static final long serialVersionUID = -1210981535415341283L;

    //набор атрибутов со значениями
    private Collection<T> featureset;

    //категория
    private K category;

    //вероятность
    private float probability;

    //конструктор класса
    public Classification(Collection<T> featureset, K category) {
        this(featureset, category, 1.0f);
    }

    //конструктор класса
    public Classification(Collection<T> featureset, K category, float probability) {
        this.featureset = featureset;
        this.category = category;
        this.probability = probability;
    }

    /*
    методы get для получения полей
     */
    public Collection<T> getFeatureset() {
        return featureset;
    }

    public float getProbability() {
        return this.probability;
    }

    public K getCategory() {
        return category;
    }

    @Override
    public String toString() {
        return "Classification [category=" + this.category + ", probability=" + this.probability + ", featureset="
                + this.featureset + "]";
    }
}

