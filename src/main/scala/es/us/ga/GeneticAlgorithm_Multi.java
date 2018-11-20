package es.us.ga;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GeneticAlgorithm_Multi {

    // parameters for the GA
    public static final int POPULATION_SIZE = 200;
    public static final int NUM_GENERATIONS = 100;
    public static final double MUTATION_RATE = 0.1;
    public static final double MUTATION_WEIGHTS = 0.7;
    public static final double MUTATION_K = 0.3;
    public static final double CROSSOVER_RATE = 0.95;
    public static final int NUM_ELIT_CHROMOSOMES = 2;
    public static final int TOURNAMENT_SIZE = 2;
    public static int DIMENSION = 7;
    private static final int K_MAX = 11;
    public static String PATHTODATA = "";

    public Population_Multi evolve (Population_Multi polutaion){
        return mutationPopulation(crossoverPopulation(polutaion));
    }

    private Population_Multi crossoverPopulation(Population_Multi population){
        Population_Multi crossoverPopulation = new Population_Multi(population.getChromosomes().size());

        for (int i=0; i < NUM_ELIT_CHROMOSOMES; i++){
            crossoverPopulation.getChromosomes().add(i, population.getChromosomes().get(i));
        }

        for (int x=NUM_ELIT_CHROMOSOMES; x < population.getChromosomes().size(); x++){
            Chromosome_Multi chromosome1= selectTournamentPopulation(population).getChromosomes().get(0);
            Chromosome_Multi chromosome2 = selectTournamentPopulation(population).getChromosomes().get(0);
            crossoverPopulation.getChromosomes().add(x, crossoverChromosome(chromosome1,chromosome2));
        }

        return crossoverPopulation;
    }

    private Population_Multi mutationPopulation(Population_Multi population){
        Population_Multi mutatePopulation = new Population_Multi(population.getChromosomes().size());

        for (int i=0; i < NUM_ELIT_CHROMOSOMES; i++){
            mutatePopulation.getChromosomes().add(i, population.getChromosomes().get(i));
        }

        for (int x=NUM_ELIT_CHROMOSOMES; x < population.getChromosomes().size(); x++){
            if (Math.random() < MUTATION_RATE){
                //Mutar el gen entero
                mutatePopulation.getChromosomes().add(x, mutateChromosome(population.getChromosomes().get(x)));
                //Mutar solo una variable
//                mutatePopulation.getChromosomes().add(x, mutateChromosomeOnlyOneGen(population.getChromosomes().get(x)));
                //Mutar solo una variable y K
//                mutatePopulation.getChromosomes().add(x, mutateChromosomeOnlyOneGenWithK(population.getChromosomes().get(x)));
            }else {
                mutatePopulation.getChromosomes().add(x, population.getChromosomes().get(x));
            }
        }

        return mutatePopulation;
    }

    private Chromosome_Multi crossoverChromosome(Chromosome_Multi chromosome1, Chromosome_Multi chromosome2){
        Chromosome_Multi crossoverChromosome = new Chromosome_Multi(chromosome1.getGenes().length-1, K_MAX);

        for (int i=0; i < chromosome1.getGenes().length; i++){
            if (Math.random() < CROSSOVER_RATE){
                crossoverChromosome.getGenes()[i] = chromosome1.getGenes()[i];
            }else{
                crossoverChromosome.getGenes()[i] = chromosome2.getGenes()[i];
            }
        }

        return crossoverChromosome.validateChromosome();
    }

    private Chromosome_Multi mutateChromosome (Chromosome_Multi chromosome){
        Chromosome_Multi mutateChromosome = chromosome;

        for (int i=0; i < chromosome.getGenes().length; i++){
            if (i!=chromosome.getGenes().length-1){
                if (Math.random() < MUTATION_WEIGHTS){
                    if (chromosome.getGenes()[i] == 0){
                        mutateChromosome.getGenes()[i] = 1;
                    }else if(chromosome.getGenes()[i] == 1){
                        mutateChromosome.getGenes()[i] = 0;
                    }
                }else {
                    mutateChromosome.getGenes()[i] = chromosome.getGenes()[i];
                }
            }else if (i==chromosome.getGenes().length-1){
                if (Math.random() < MUTATION_K){
                    if (Math.random() < 0.5){
                        mutateChromosome.getGenes()[i] = chromosome.getGenes()[i] + 1;
                    }else if (chromosome.getGenes()[i] > 2){
                        mutateChromosome.getGenes()[i] = chromosome.getGenes()[i] - 1;
                    }
                }else{
                    mutateChromosome.getGenes()[i] = chromosome.getGenes()[i];
                }
            }
        }

        return  mutateChromosome.validateChromosome();
    }

    private Chromosome_Multi mutateChromosomeOnlyOneGen (Chromosome_Multi chromosome){
        Chromosome_Multi mutateChromosome = chromosome;

        if (Math.random() < MUTATION_WEIGHTS){
            int index = ThreadLocalRandom.current().nextInt(0, chromosome.getGenes().length-1);
            if (chromosome.getGenes()[index] == 0){
                mutateChromosome.getGenes()[index] = 1;
            }else if(chromosome.getGenes()[index] == 1){
                mutateChromosome.getGenes()[index] = 0;
            }
        }

        return  mutateChromosome.validateChromosome();
    }

    private Chromosome_Multi mutateChromosomeOnlyOneGenWithK (Chromosome_Multi chromosome){
        Chromosome_Multi mutateChromosome = chromosome;
        int indexK = chromosome.getGenes().length-1;
        if (Math.random() < MUTATION_WEIGHTS){
            int index = ThreadLocalRandom.current().nextInt(0, indexK);
            if (chromosome.getGenes()[index] == 0){
                mutateChromosome.getGenes()[index] = 1;
            }else if(chromosome.getGenes()[index] == 1){
                mutateChromosome.getGenes()[index] = 0;
            }
        }

        if (Math.random() < MUTATION_K) {
            if (Math.random() < 0.5) {
                mutateChromosome.getGenes()[indexK] = chromosome.getGenes()[indexK] + 1;
            } else if (chromosome.getGenes()[indexK] > 2) {
                mutateChromosome.getGenes()[indexK] = chromosome.getGenes()[indexK] - 1;
            }
        }

        return  mutateChromosome.validateChromosome();
    }

    private Population_Multi selectTournamentPopulation (Population_Multi population){
        Population_Multi tournamentPopulation = new Population_Multi(TOURNAMENT_SIZE);

        for (int i=0; i < TOURNAMENT_SIZE; i++){
            tournamentPopulation.getChromosomes().add(i, population.getChromosomes().get((int) (Math.random()*population.getChromosomes().size())));
        }

        tournamentPopulation.sortChromosomesByFitness();

        return tournamentPopulation;
    }

    public Population_Multi randomPopulation() {
        List<Chromosome_Multi> popList = new LinkedList<>();

        for(int x=0; x < POPULATION_SIZE; x++){
            int k = ThreadLocalRandom.current().nextInt(2,K_MAX);
            Chromosome_Multi newChromosome = new Chromosome_Multi(DIMENSION,k).inicializeChromosome().validateInitialChromosome();
            popList.add(newChromosome);
        }

        return new Population_Multi(popList);
    }

    public Population_Multi substitutionPoblation (Population_Multi oldPopulation){
        List<Chromosome_Multi> popList = new LinkedList<>();

        for(int x=0; x < POPULATION_SIZE; x++){

            if (x < (POPULATION_SIZE/2) ){
                popList.add(oldPopulation.getChromosomes().get(x));
            }else {
                int k = ThreadLocalRandom.current().nextInt(2,K_MAX);
                Chromosome_Multi newChromosome = new Chromosome_Multi(DIMENSION,k).inicializeChromosome().validateChromosome();
                popList.add(newChromosome);
            }
        }

        return new Population_Multi(popList);
    }

    public void setPATHTODATA(String path) {
        PATHTODATA = path;
    }

    public void setDimension(Integer dimension) {
        DIMENSION = dimension;
    }
}
