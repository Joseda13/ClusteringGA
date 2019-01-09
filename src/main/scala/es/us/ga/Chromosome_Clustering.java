package es.us.ga;

import es.us.spark.mllib.clustering.validation.Indices;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Chromosome_Clustering {

    private boolean isFitnessChanged = true;
    private double fitness = 0;
    private int[] genes;
    private int K;
    public int nv;

    public Chromosome_Clustering(int dimension, int K_max){

        genes = new int[dimension+1];
        K = K_max;
        genes[dimension] = K;

    }

    public static Chromosome_Clustering create_Chromosome_Clustering_From_String(String chromosome){

        String aux = chromosome.replace("[","").replace("]","");
        String[] elements = aux.split(",");

        Chromosome_Clustering result = new Chromosome_Clustering(elements.length-1, Integer.parseInt(elements[elements.length-1]));

        for (int index = 0; index < elements.length - 1; index++){
            if (Integer.parseInt(elements[index]) == 1){
                result.getGenes()[index] = 1;
            }else {
                result.getGenes()[index] = 0;
            }
        }

        return result;

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

    public Chromosome_Clustering inicializeChromosomeWithDummiesRandom(int numberDummies){

        for (int x = 0; x < nv; x++){
            genes[x] = 1;
        }

        if (numberDummies != 0 ) {
                int dummie = ThreadLocalRandom.current().nextInt(nv, genes.length - 1);
                int dummie2 = ThreadLocalRandom.current().nextInt(nv, genes.length - 1);
                genes[dummie] = 1;
                genes[dummie2] = 1;
        }

        return this;

    }

    public Chromosome_Clustering inicializeChromosome (){

        for (int x = 0; x < genes.length - 1; x++){
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

    public Chromosome_Clustering validateInitialChromosome(){

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

    public double getFitness(){

        if (isFitnessChanged){
            fitness = recalculatedFitness();
            isFitnessChanged = false;
        }

        return fitness;

    }

    public double recalculatedFitness(){

        //Fitness = Dunn
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm.DATABASE);
        //Fitness = Dunn + sqrt(nv)
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm.DATABASE) + Math.sqrt(contAttributesAll());
        //Fitness = Dunn + nv
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm.DATABASE) + contAttributesAll();
        //Fitness = Dunn + ln(nv)
//        return Indices.getFitnessDunn(getGenes(), GeneticAlgorithm.DATABASE) + Math.log(contAttributesAll());
        //Fitness = Silhoutte
//        return Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm.DATABASE);
        //Fitness = Silhoutte + (1-(1(/nv)))
//        return (Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm.DATABASE) + (1.0 - (1.0 / contAttributesAll()) ));
        //Fitness = Silhoutte + 0.1*(1-(1(/nv)))
//        return (Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm.DATABASE) + (0.1*(1.0 - (1.0 / contAttributesAll()))) );
        //Fitness = Silhoutte + 0.75*(1-(1(/nv)))
        return (Indices.getFitnessSilhouette(getGenes(), GeneticAlgorithm.DATABASE) + (0.75*(1.0 - (1.0 / contAttributesAll()))) );

    }

    public String toString(){

        return "[f="+ this.getFitness() + "] " + Arrays.toString(this.genes);

    }

    public String toSpecialString() {

        return Arrays.toString(this.genes);

    }

}
