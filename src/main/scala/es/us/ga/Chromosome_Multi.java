package es.us.ga;

import es.us.spark.mllib.clustering.validation.Indices;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class Chromosome_Multi {

    private boolean isFitnessChanged = true;
    private boolean isDunnCalculated;
    private boolean isSilhoutteCalculated;
    private boolean isCorrectionCalculated;

    private double dunnValue = 0d;
    private double silhoutteValue = 0d;
    private double dimensionCorrection = 0d;
    private double numberDominations = 0d;
    private double maxEuclideanDistance = 0d;
    private double fitness = 0d;

    private int[] genes;
    private int K;

    public Chromosome_Multi(int dimension, int K_max){
        genes = new int[dimension+1];
        K = K_max;
        genes[dimension] = K;
        isDunnCalculated = false;
        isSilhoutteCalculated = false;
        isCorrectionCalculated = false;
    }

    public int contAttributesAll(){
        int cont = 0;

        for (int x = 0; x < genes.length - 1; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        return cont;
    }

    public Chromosome_Multi inicializeChromosome (){
        for (int x = 0; x < genes.length - 1; x++){
            if (Math.random() >= 0.5){
                genes[x] = 1;
            }else {
                genes[x] = 0;
            }
        }

        return this;
    }

    public Chromosome_Multi validateChromosome(){
        int cont = 0;

        for (int x =0; x < genes.length - 1; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        if (cont == 0){
            int index = ThreadLocalRandom.current().nextInt(0, genes.length-1);
            genes[index] = 1;
        }

        return this;
    }

    public Chromosome_Multi validateInitialChromosome(){
        int cont = 0;

        for (int x =0; x < genes.length - 1; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        if (cont == 0 || cont < (0.2 * genes.length)){
            int index = ThreadLocalRandom.current().nextInt(0, genes.length-1);
            genes[index] = 1;
            validateInitialChromosome();
        }

        return this;
    }

    public int[] getGenes(){
        return genes;
    }

    public double getDunnValue(){
        return dunnValue;
    }

    public void setDunnValue(){
        if (!isDunnCalculated){
//            dunnValue = Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Multi.PATHTODATA) + (0.1*(1.0 - (1.0 / contAttributesAll())));
            dunnValue = Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Multi.PATHTODATA) + (Math.sqrt(contAttributesAll()));
//            dunnValue = Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Multi.PATHTODATA);
            isDunnCalculated = true;
        }
    }

    public double getSilhoutteValue(){
        return silhoutteValue;
    }

    public void setSilhoutteValue(){
        if(!isSilhoutteCalculated){
            silhoutteValue = Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm_Multi.PATHTODATA) + (0.5*(1.0 - (1.0 / contAttributesAll()) ));
//            silhoutteValue = Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm_Multi.PATHTODATA);
            isSilhoutteCalculated = true;
        }
    }

    public double getDimensionCorrection(){
        return dimensionCorrection;
    }

    public void setDimensionCorrection(){
        if(!isCorrectionCalculated){
            dimensionCorrection = 0.1*(1.0 - (1.0 / contAttributesAll()) );
//            dimensionCorrection = Math.sqrt(contAttributesAll());
            isCorrectionCalculated = true;
        }
    }

    public void updateObjetives(){
        setDunnValue();
        setSilhoutteValue();
//        setDimensionCorrection();
    }

    public void setNumberDominations(double numberDom){
        numberDominations = numberDom;
    }

    public void setMaxEuclideanDistance(double dist){
        maxEuclideanDistance = dist;
    }

    public double getFitness(){
//        if (isFitnessChanged){
//            fitness = recalculatedFitness();
//            isFitnessChanged = false;
//        }
//        return fitness;
        return (numberDominations*1.0) + maxEuclideanDistance;
    }

    public double recalculatedFitness(){
        return (numberDominations*1.0) + maxEuclideanDistance;
    }

    public String toString(){
        return "[f="+ this.getFitness() + "]" + "[n="+ numberDominations + "]"
                + "[dist="+ maxEuclideanDistance + "]"+ "[d="+ this.getDunnValue() + "]" +
                "[sil="+ this.getSilhoutteValue() + "]" + "[cor="+ this.getDimensionCorrection() + "]"
                + Arrays.toString(this.genes);
    }

    public String toSpecialString() {
        return Arrays.toString(this.genes);
    }
}
