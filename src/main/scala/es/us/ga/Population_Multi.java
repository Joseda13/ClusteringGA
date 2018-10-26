package es.us.ga;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Population_Multi {

    private List<Chromosome_Multi> chromosomes;

    public Population_Multi(int length){
        chromosomes = new ArrayList<>(length);
    }

    public Population_Multi(List<Chromosome_Multi> popList){
        chromosomes = popList;
    }

    public List<Chromosome_Multi> getChromosomes(){
        return chromosomes;
    }

    public void updateChromosomes(){
        chromosomes.parallelStream().forEach(x -> x.updateObjetives());
    }

    public void calculatedObjetivesValues(){

        int resultNumberDominations = 0;
        double resultMaxEuclideanDistance = 0d;

        for (int i = 0; i < chromosomes.size() - 1; i++){

            Chromosome_Multi chromo_i = chromosomes.get(i);

            double dunnValueChromo_i = chromo_i.getDunnValue();
            double silhoutteValueChromo_i = chromo_i.getSilhoutteValue();
            double correctionValueChromo_i = chromo_i.getDimensionCorrection();

            int notWorse = 0;
            double euclideanDistance = 0d;

            for (int j = 0; j < chromosomes.size() - 1; j++){

                Chromosome_Multi chromo_j = chromosomes.get(j);

                if (i != j){

                    double dunnValueChomo_j = chromo_j.getDunnValue();
                    double silhoutteValueChromo_j = chromo_j.getSilhoutteValue();
                    double correctionValueChromo_j = chromo_j.getDimensionCorrection();

                    if ( (dunnValueChromo_i < dunnValueChomo_j) || (silhoutteValueChromo_i < silhoutteValueChromo_j) || (correctionValueChromo_i < correctionValueChromo_j) ){
                        notWorse += 1;
                    }

                    if ( ((dunnValueChromo_i > dunnValueChomo_j) || (silhoutteValueChromo_i > silhoutteValueChromo_j)
                            || (correctionValueChromo_i > correctionValueChromo_j)) && (notWorse == 0) ) {
                        resultNumberDominations += 1;
                    }

                    double diferenceDunnValue = dunnValueChromo_i - dunnValueChomo_j;
                    double diferenceSilhoutteValue = silhoutteValueChromo_i - silhoutteValueChromo_j;
                    double diferenceCorrectionValue = correctionValueChromo_i - correctionValueChromo_j;

                    euclideanDistance = Math.pow(diferenceDunnValue, 2) + Math.pow(diferenceSilhoutteValue, 2) + Math.pow(diferenceCorrectionValue, 2);

                    if (resultMaxEuclideanDistance != 0){
                        if ( euclideanDistance > resultMaxEuclideanDistance){
                            resultMaxEuclideanDistance = euclideanDistance;
                        }
                    } else {
                        resultMaxEuclideanDistance = euclideanDistance;
                    }
                }
            }

            chromo_i.setNumberDominations(resultNumberDominations);
            chromo_i.setMaxEuclideanDistance(resultMaxEuclideanDistance);
        }
    }

    public void sortChromosomesByFitness(){

        Comparator<Chromosome_Multi> byFitness =
                (Chromosome_Multi c1, Chromosome_Multi c2)-> {
                    if (c1.getFitness() < c2.getFitness()) return 1;
                    if (c1.getFitness() > c2.getFitness()) return -1;
                    return 0;
                };

        chromosomes.sort(byFitness);
    }
}
