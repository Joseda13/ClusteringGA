package es.us.ga;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Population_Clustering {
    private List<Chromosome_Clustering> chromosomes;

    public Population_Clustering(int length){
        chromosomes = new ArrayList<>(length);
    }


    public Population_Clustering(List<Chromosome_Clustering> popList){
        chromosomes = popList;
    }

    public List<Chromosome_Clustering> getChromosomes(){
        return chromosomes;
    }

    public void sortChromosomesByFitness(){
        chromosomes.parallelStream().forEach(x -> x.getFitness());

        Comparator<Chromosome_Clustering> byFitness =
                (Chromosome_Clustering c1, Chromosome_Clustering c2)-> {
//                    System.out.println("Chromo 1: " + c1.toString());
//                    System.out.println("Chromo 2: " + c2.toString());
                    if (c1.getFitness() < c2.getFitness()) return 1;
                    if (c1.getFitness() > c2.getFitness()) return -1;
                    if (c1.getFitness() == c2.getFitness()) return 1;
                    return 0;
                };

        chromosomes.sort(byFitness);
    }
}
