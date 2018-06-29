package es.us.ga;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GeneticAlgorithm_Example {

    public static final int POPULATION_SIZE = 100;
    private static final double MUTATION_RATE = 0.08;
    private static final double CROSSOVER_RATE = 0.95;
    public static final int NUM_ELIT_CHROMOSOMES = 2;
    public static final int TOURNAMENT_SIZE = 2;
    private static final int DIMENSION = 4;
    private static final int K_MAX = 10;

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
            mutatePopulation.getChromosomes().add(x, mutateChromosome(population.getChromosomes().get(x)));
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

        return crossoverChromosome;
    }

    private Chromosome_Clustering mutateChromosome (Chromosome_Clustering chromosome){
//        int k = ThreadLocalRandom.current().nextInt(2,K_MAX);
        Chromosome_Clustering mutateChromosome = new Chromosome_Clustering(chromosome.getGenes().length-1, chromosome.getGenes()[chromosome.getGenes().length-1]);

        for (int i=0; i < chromosome.getGenes().length - 1; i++){
            if (Math.random() < MUTATION_RATE){
                if (Math.random() < 0.5){
                    mutateChromosome.getGenes()[i] = 1;
                }else {
                    mutateChromosome.getGenes()[i] = 0;
                }
            }else {
                mutateChromosome.getGenes()[i] = chromosome.getGenes()[i];
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

        for(int x=0; x < GeneticAlgorithm_Example.POPULATION_SIZE; x++){
            int k = ThreadLocalRandom.current().nextInt(2,K_MAX);
            Chromosome_Clustering newChromosome = new Chromosome_Clustering(DIMENSION,k).inicializeChromosome().validateChromosome();
            popList.add(newChromosome);
        }

        return new Population_Clustering(popList);
    }
}
