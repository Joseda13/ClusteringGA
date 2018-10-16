package es.us.ga;

import es.us.spark.mllib.clustering.validation.FeatureStatistics;
import es.us.spark.mllib.clustering.validation.Indices;
import org.apache.commons.math3.genetics.AbstractListChromosome;
import org.apache.commons.math3.genetics.BinaryChromosome;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.RandomKey;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Chromosome_Clustering {

    private boolean isFitnessChanged = true;
    private double fitness = 0;
    private int[] genes;
    private int K;
    public int nv = 5;

    public Chromosome_Clustering(int dimension, int K_max){
        genes = new int[dimension+1];
        K = K_max;
        genes[dimension] = K;
    }

    public void setNV (int numberVariables){
        nv = numberVariables;
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

    public int contAttributeGood(){
        int cont = 0;

        for (int x = 0; x < nv; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        return cont;
    }

    public int contDummies(){
        int cont = 0;

        for (int x = nv; x < genes.length - 1; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        return cont;
    }

    public Chromosome_Clustering inicializeChromosome (){
        for (int x = 0; x < genes.length - 1; x++){
//        for (int x = 0; x < nv; x++){
            if (Math.random() >= 0.5){
                genes[x] = 1;
            }else {
                genes[x] = 0;
            }
        }

//        int dummy1 = ThreadLocalRandom.current().nextInt(nv, genes.length-1);
//        int dummy2 = ThreadLocalRandom.current().nextInt(nv, genes.length-1);
//        int dummy3 = ThreadLocalRandom.current().nextInt(nv, genes.length-1);
//            if (Math.random() >= 0.5){
//                genes[dummy1] = 1;
//            }else {
//                genes[dummy1] = 0;
//            }
//
//            if (Math.random() >= 0.5){
//                genes[dummy2] = 1;
//            }else {
//                genes[dummy2] = 0;
//            }

//            if (Math.random() >= 0.5){
//                genes[dummy3] = 1;
//            }else {
//                genes[dummy3] = 0;
//            }

        return this;
    }

    public static List<Chromosome_Clustering> mainTestChromosomes (int dimension){
        List<Chromosome_Clustering> popList = new LinkedList<>();

        Chromosome_Clustering c1 = new Chromosome_Clustering(dimension,7);
        c1.getGenes()[0] = 1;
        c1.getGenes()[1] = 1;
        c1.getGenes()[2] = 1;
        c1.getGenes()[3] = 1;
        c1.getGenes()[4] = 1;
        c1.getGenes()[5] = 0;
        c1.getGenes()[6] = 0;
//        c1.getGenes()[7] = 0;
//        c1.getGenes()[8] = 1;
//        c1.getGenes()[9] = 0;
//        c1.getGenes()[10] = 0;
//        c1.getGenes()[11] = 0;
//        c1.getGenes()[12] = 0;
//        c1.getGenes()[13] = 0;

//        Chromosome_Clustering c2 = new Chromosome_Clustering(dimension,4);
//        c2.getGenes()[0] = 1;
//        c2.getGenes()[1] = 0;
//        c2.getGenes()[2] = 0;
//        c2.getGenes()[3] = 0;
//        c2.getGenes()[4] = 0;
//        c2.getGenes()[5] = 0;
//        c2.getGenes()[6] = 0;
//        c2.getGenes()[7] = 0;
//        c2.getGenes()[8] = 1;
//        c2.getGenes()[9] = 1;
//        c2.getGenes()[10] = 1;
//        c2.getGenes()[11] = 0;
//        c2.getGenes()[12] = 0;
//        c2.getGenes()[13] = 0;
//
//        Chromosome_Clustering c3 = new Chromosome_Clustering(dimension,4);
//        c3.getGenes()[0] = 0;
//        c3.getGenes()[1] = 0;
//        c3.getGenes()[2] = 1;
//        c3.getGenes()[3] = 0;
//        c3.getGenes()[4] = 0;
//        c3.getGenes()[5] = 0;
//        c3.getGenes()[6] = 1;
//        c3.getGenes()[7] = 0;
//        c3.getGenes()[8] = 0;
//        c3.getGenes()[9] = 1;
//        c3.getGenes()[10] = 0;
//        c3.getGenes()[11] = 0;
//        c3.getGenes()[12] = 1;
//        c3.getGenes()[13] = 0;
//
//        Chromosome_Clustering c4 = new Chromosome_Clustering(dimension,4);
//        c4.getGenes()[0] = 0;
//        c4.getGenes()[1] = 0;
//        c4.getGenes()[2] = 0;
//        c4.getGenes()[3] = 1;
//        c4.getGenes()[4] = 1;
//        c4.getGenes()[5] = 1;
//        c4.getGenes()[6] = 1;
//        c4.getGenes()[7] = 0;
//        c4.getGenes()[8] = 0;
//        c4.getGenes()[9] = 0;
//        c4.getGenes()[10] = 0;
//        c4.getGenes()[11] = 0;
//        c4.getGenes()[12] = 0;
//        c4.getGenes()[13] = 0;
//
//        Chromosome_Clustering c5 = new Chromosome_Clustering(dimension,4);
//        c5.getGenes()[0] = 0;
//        c5.getGenes()[1] = 1;
//        c5.getGenes()[2] = 0;
//        c5.getGenes()[3] = 0;
//        c5.getGenes()[4] = 0;
//        c5.getGenes()[5] = 1;
//        c5.getGenes()[6] = 0;
//        c5.getGenes()[7] = 0;
//        c5.getGenes()[8] = 1;
//        c5.getGenes()[9] = 0;
//        c5.getGenes()[10] = 1;
//        c5.getGenes()[11] = 0;
//        c5.getGenes()[12] = 0;
//        c5.getGenes()[13] = 0;
//
//        Chromosome_Clustering c6 = new Chromosome_Clustering(dimension,4);
//        c6.getGenes()[0] = 0;
//        c6.getGenes()[1] = 0;
//        c6.getGenes()[2] = 1;
//        c6.getGenes()[3] = 0;
//        c6.getGenes()[4] = 1;
//        c6.getGenes()[5] = 0;
//        c6.getGenes()[6] = 1;
//        c6.getGenes()[7] = 0;
//        c6.getGenes()[8] = 0;
//        c6.getGenes()[9] = 0;
//        c6.getGenes()[10] = 0;
//        c6.getGenes()[11] = 1;
//        c6.getGenes()[12] = 0;
//        c6.getGenes()[13] = 0;
//
//        Chromosome_Clustering c7 = new Chromosome_Clustering(dimension,4);
//        c7.getGenes()[0] = 1;
//        c7.getGenes()[1] = 0;
//        c7.getGenes()[2] = 0;
//        c7.getGenes()[3] = 0;
//        c7.getGenes()[4] = 0;
//        c7.getGenes()[5] = 0;
//        c7.getGenes()[6] = 0;
//        c7.getGenes()[7] = 1;
//        c7.getGenes()[8] = 0;
//        c7.getGenes()[9] = 0;
//        c7.getGenes()[10] = 0;
//        c7.getGenes()[11] = 1;
//        c7.getGenes()[12] = 1;
//        c7.getGenes()[13] = 0;

        popList.add(c1);
//        popList.add(c2);
//        popList.add(c3);
//        popList.add(c4);
//        popList.add(c5);
//        popList.add(c6);
//        popList.add(c7);

        return popList;
    }

    public Chromosome_Clustering validateChromosome(){
        int cont = 0;

        for (int x =0; x < genes.length - 1; x++){
            if (genes[x] == 1){
                cont++;
            }
        }

        if (cont == 0 || cont < (0.2 * genes.length)){
            int index = ThreadLocalRandom.current().nextInt(0, genes.length-1);
            genes[index] = 1;
            validateChromosome();
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
        //Fitness = Dunn
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Example.PATHTODATA);
        //Fitness = Dunn + sqrt(nv)
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Example.PATHTODATA) + Math.sqrt(contAttributesAll());
        //Fitness = Dunn + nv
        // return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Example.PATHTODATA) + contAttributesAll();
        //Fitness = Dunn + ln(nv)
        //return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm_Example.PATHTODATA) + Math.ln(1 + contAttributesAll() );
        //Fitness = Silhoutte
//        return Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm_Example.PATHTODATA);
        //Fitness = Silhoutte + (1-(1(/nv)))
        return (Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm_Example.PATHTODATA) + (1.0 - (1.0 / contAttributesAll()) ));
    }

    public String toString(){
        return "[f="+ this.getFitness() + "] " + Arrays.toString(this.genes);
    }

    public String toSpecialString() {
        return Arrays.toString(this.genes);
    }

}
