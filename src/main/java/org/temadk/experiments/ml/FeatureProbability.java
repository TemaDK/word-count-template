package org.temadk.experiments.ml;

import java.io.Serializable;

public interface FeatureProbability<T, K> extends Serializable {

    //метода расчета вероятности пересечения категории и признака
    public float featureProbability(T feature, K category);

}

