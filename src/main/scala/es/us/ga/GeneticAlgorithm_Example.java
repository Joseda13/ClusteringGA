package es.us.ga;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GeneticAlgorithm_Example {

    // parameters for the GA
    public static final int POPULATION_SIZE = 97;
    public static final int NUM_GENERATIONS = 5;
    public static final double MUTATION_RATE = 0.08;
    public static final double MUTATION_WEIGHTS = 0.7;
    public static final double MUTATION_K = 0.3;
    public static final double CROSSOVER_RATE = 0.95;
    public static final int NUM_ELIT_CHROMOSOMES = 2;
    public static final int TOURNAMENT_SIZE = 2;
    private static final int DIMENSION = 6;
    private static final int K_MAX = 10;
    public static String PATHTODATA= "B:\\DataSets_Genetics\\dataset_104.csv";

    public Population_Clustering evolve (Population_Clustering polutaion){
        return mutationPopulation(crossoverPopulation(polutaion));
    }

    private Population_Clustering crossoverPopulation(Population_Clustering population){
        Population_Clustering crossoverPopulation = new Population_Clustering(population.getChromosomes().size());

        for (int i=0; i < NUM_ELIT_CHROMOSOMES; i++){
            crossoverPopulation.getChromosomes().add(i, population.getChromosomes().get(i));
        }

        for (int x=NUM_ELIT_CHROMOSOMES; x < population.getChromosomes().size(); x++){
            Chromosome_Clustering chromosome1= selectTournamentPopulation(population).getChromosomes().get(0);
            Chromosome_Clustering chromosome2 = selectTournamentPopulation(population).getChromosomes().get(0);
            crossoverPopulation.getChromosomes().add(x, crossoverChromosome(chromosome1,chromosome2));
        }

        return crossoverPopulation;
    }

    private Population_Clustering mutationPopulation(Population_Clustering population){
        Population_Clustering mutatePopulation = new Population_Clustering(population.getChromosomes().size());

        for (int i=0; i < NUM_ELIT_CHROMOSOMES; i++){
            mutatePopulation.getChromosomes().add(i, population.getChromosomes().get(i));
        }

        for (int x=NUM_ELIT_CHROMOSOMES; x < population.getChromosomes().size(); x++){
            if (Math.random() < MUTATION_RATE){
                mutatePopulation.getChromosomes().add(x, mutateChromosome(population.getChromosomes().get(x)));
            }else {
                mutatePopulation.getChromosomes().add(x, population.getChromosomes().get(x));
            }
        }

        return mutatePopulation;
    }

    private Chromosome_Clustering crossoverChromosome(Chromosome_Clustering chromosome1, Chromosome_Clustering chromosome2){
        Chromosome_Clustering crossoverChromosome = new Chromosome_Clustering(chromosome1.getGenes().length-1, K_MAX);

        for (int i=0; i < chromosome1.getGenes().length; i++){
            if (Math.random() < CROSSOVER_RATE){
                crossoverChromosome.getGenes()[i] = chromosome1.getGenes()[i];
            }else{
                crossoverChromosome.getGenes()[i] = chromosome2.getGenes()[i];
            }
        }

        return crossoverChromosome.validateChromosome();
    }

    private Chromosome_Clustering mutateChromosome (Chromosome_Clustering chromosome){
        Chromosome_Clustering mutateChromosome = chromosome;

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
            }else {
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

    private Population_Clustering selectTournamentPopulation (Population_Clustering population){
        Population_Clustering tournamentPopulation = new Population_Clustering(TOURNAMENT_SIZE);

        for (int i=0; i < TOURNAMENT_SIZE; i++){
            tournamentPopulation.getChromosomes().add(i, population.getChromosomes().get((int) (Math.random()*population.getChromosomes().size())));
        }

        tournamentPopulation.sortChromosomesByFitness();

        return tournamentPopulation;
    }

    public Population_Clustering randomPopulation() {
        List<Chromosome_Clustering> popList = new LinkedList<>();

//        for(int x=0; x < GeneticAlgorithm_Example.POPULATION_SIZE; x++){
//            int k = ThreadLocalRandom.current().nextInt(2,K_MAX);
//            Chromosome_Clustering newChromosome = new Chromosome_Clustering(DIMENSION,k).inicializeChromosome().validateChromosome();
//            popList.add(newChromosome);
//        }

        List<Chromosome_Clustering> auxList = Chromosome_Clustering.mainTestChromosomes(10);
        popList.add(auxList.get(0));
        popList.add(auxList.get(1));
        popList.add(auxList.get(2));
        popList.add(auxList.get(3));
        popList.add(auxList.get(4));
        popList.add(auxList.get(5));
        popList.add(auxList.get(6));
        popList.add(auxList.get(7));
        popList.add(auxList.get(8));
        popList.add(auxList.get(9));

        return new Population_Clustering(popList);
    }
}
