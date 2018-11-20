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

    public void calculatedObjetivesValuesStack(){

        double maxDunnPoblation = 0d;
        double minDunnPoblation = 0d;

        double maxSilhouttePoblation = 0d;
        double minSilhouttePoblation = 0d;

//        double maxCorrectionPoblation = 0d;
//        double minCorrectionPoblation = 0d;

        for (int x = 0; x < chromosomes.size(); x++){

            Chromosome_Multi chromo_x = chromosomes.get(x);

            double dunnX = chromo_x.getDunnValue();
            double silhoutteX = chromo_x.getSilhoutteValue();
//            double correctionX = chromo_x.getDimensionCorrection();

            if (maxDunnPoblation != 0){
                if ( dunnX > maxDunnPoblation){
                    maxDunnPoblation = dunnX;
                }
            } else {
                maxDunnPoblation = dunnX;
            }

            if (minDunnPoblation != 0){
                if ( dunnX < minDunnPoblation){
                    minDunnPoblation = dunnX;
                }
            } else {
                minDunnPoblation = dunnX;
            }

            if (maxSilhouttePoblation != 0){
                if ( silhoutteX > maxSilhouttePoblation){
                    maxSilhouttePoblation = silhoutteX;
                }
            } else {
                maxSilhouttePoblation = silhoutteX;
            }

            if (minSilhouttePoblation != 0){
                if ( silhoutteX < minSilhouttePoblation){
                    minSilhouttePoblation = silhoutteX;
                }
            } else {
                minSilhouttePoblation = silhoutteX;
            }

//            if (maxCorrectionPoblation != 0){
//                if ( correctionX > maxCorrectionPoblation){
//                    maxCorrectionPoblation = correctionX;
//                }
//            } else {
//                maxCorrectionPoblation = correctionX;
//            }
//
//            if (minCorrectionPoblation != 0){
//                if ( correctionX < minCorrectionPoblation){
//                    minCorrectionPoblation = correctionX;
//                }
//            } else {
//                minCorrectionPoblation = correctionX;
//            }
        }

        for (int i = 0; i < chromosomes.size(); i++){

            int resultNumberDominations = 0;
            double resultMaxEuclideanDistance = 0d;

            Chromosome_Multi chromo_i = chromosomes.get(i);

            double dunnValueChromo_i = chromo_i.getDunnValue();
            double silhoutteValueChromo_i = chromo_i.getSilhoutteValue();
//            double correctionValueChromo_i = chromo_i.getDimensionCorrection();

            for (int j = 0; j < chromosomes.size(); j++){

                int notWorse = 0;

                Chromosome_Multi chromo_j = chromosomes.get(j);

                if (i != j){

                    double dunnValueChomo_j = chromo_j.getDunnValue();
                    double silhoutteValueChromo_j = chromo_j.getSilhoutteValue();
//                    double correctionValueChromo_j = chromo_j.getDimensionCorrection();

                    if ( (dunnValueChromo_i < dunnValueChomo_j) || (silhoutteValueChromo_i < silhoutteValueChromo_j)
//                            || (correctionValueChromo_i < correctionValueChromo_j)
                            ){
                        notWorse += 1;
                    }

                    if ( ((dunnValueChromo_i > dunnValueChomo_j) || (silhoutteValueChromo_i > silhoutteValueChromo_j)
//                            || (correctionValueChromo_i > correctionValueChromo_j)
                    ) && (notWorse == 0) ) {
                        resultNumberDominations += 1;
                    }

                    double diferenceDunnValue = dunnValueChromo_i - dunnValueChomo_j;
                    double resultDunn = diferenceDunnValue / (maxDunnPoblation - minDunnPoblation);

                    double diferenceSilhoutteValue = silhoutteValueChromo_i - silhoutteValueChromo_j;
                    double resultSilhoutte = diferenceSilhoutteValue / (maxSilhouttePoblation - minSilhouttePoblation);

//                    double diferenceCorrectionValue = correctionValueChromo_i - correctionValueChromo_j;
//                    double resultCorrection = diferenceCorrectionValue / (maxCorrectionPoblation - minCorrectionPoblation);

                    double euclideanDistance = Math.abs(resultDunn) + Math.abs(resultSilhoutte);
//                            + Math.abs(resultCorrection)

                    if (resultMaxEuclideanDistance != 0){
                        if ( euclideanDistance > resultMaxEuclideanDistance){
                            resultMaxEuclideanDistance = euclideanDistance;
                        }
                    } else {
                        resultMaxEuclideanDistance = euclideanDistance;
                    }
                }
            }

            chromo_i.setNumberDominations((resultNumberDominations * 1.0) / (GeneticAlgorithm_Multi.POPULATION_SIZE - 1) );
//            chromo_i.setNumberDominations(resultNumberDominations);
            chromo_i.setMaxEuclideanDistance(resultMaxEuclideanDistance / 2.0);
        }
    }

    public void calculatedObjetivesValues(){

        for (int i = 0; i < chromosomes.size(); i++){

            int resultNumberDominations = 0;
            double resultMaxEuclideanDistance = 0d;

            Chromosome_Multi chromo_i = chromosomes.get(i);

            double dunnValueChromo_i = chromo_i.getDunnValue();
            double silhoutteValueChromo_i = chromo_i.getSilhoutteValue();
            double correctionValueChromo_i = chromo_i.getDimensionCorrection();

            for (int j = 0; j < chromosomes.size(); j++){

                int notWorse = 0;

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

//                    double diferenceDunnValue = dunnValueChromo_i - dunnValueChomo_j;
                    double diferenceSilhoutteValue = silhoutteValueChromo_i - silhoutteValueChromo_j;
                    double diferenceCorrectionValue = correctionValueChromo_i - correctionValueChromo_j;

                    double euclideanDistance = Math.pow(diferenceSilhoutteValue, 2) + Math.pow(diferenceCorrectionValue, 2);

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
