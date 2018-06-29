package es.us.ga;

import es.us.spark.mllib.clustering.validation.FeatureStatistics;
import org.apache.commons.math3.genetics.AbstractListChromosome;
import org.apache.commons.math3.genetics.BinaryChromosome;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.RandomKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Chromosome_Clustering {

    private boolean isFitnessChanged = true;
    private double fitness = 0;
    private int[] genes;
    private int K;

    public Chromosome_Clustering(int dimension, int K_max){
        genes = new int[dimension+1];
        K = K_max;
        genes[dimension] = K;
    }

    public Chromosome_Clustering inicializeChromosome (){

        for (int x =0; x < genes.length - 1; x++){
            if (Math.random() >= 0.5){
                genes[x] = 1;
            }else {
                genes[x] = 0;
            }
        }

        return this;
    }

    public Chromosome_Clustering validateChromosome(){
        int cont = 0;

        for (int x =0; x < genes.length - 1; x++){
            if (genes[x] == 0){
                cont++;
            }
        }

        if (cont == genes.length - 1){
            int k = ThreadLocalRandom.current().nextInt(0, genes.length-1);
            genes[k] = 1;
        }

        return this;
    }

    public int[] getGenes(){
//        isFitnessChanged = true;
        return genes;
    }

    public double getFitness(){
        if (isFitnessChanged){
            fitness = recalculatedFitness();
            isFitnessChanged = false;
        }
        return fitness;
    }

    public double recalculatedFitness(){
        return -FeatureStatistics.getFitness(getGenes());
//        return 0;
    }

    public String toString(){
        return "[f="+ this.getFitness() + "] " + Arrays.toString(this.genes);
    }

}
