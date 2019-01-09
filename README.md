# ClusteringGA
This package contains the code for the execution of a genetic algorithm in Scala whose objective is to find the optimal number of clusters within a dataset, as well as the most relevant attributes to achieve it. The algorithm executes the K-Means method designed in Spark within its Machine Learning library (MLlib) to perform clustering. While for the calculation of the fitness function within the GA, there are several implementations of the internal validation indices for clustering of both Silhouette and Dunn.
## Getting Started
Within the "data" folder a total of 82 datasets are available to the user for testing with the implemented algorithm. Of which 65 have been generated internally thanks to our generator (https://github.com/Joseda13/ClusteringDBGenerator) and the other 17 have been obtained from the following link: http://cs.joensuu.fi/sipu/datasets/.
### Prerequisites
The package is ready to be used. You only have to download it and import it into your workspace. The main files include an example main that could be used.
As for the classes with the most important code, we can find the following:
* GeneticAlgorithm: Main class for the operation of the GA, where you will find the parameters that define its configuration.
* Population_Clustering: A class that stores information relevant to the population from our algorithm, with methods such as the management of all individuals within that population.
* Chromosome_Clustering: Class with all the information related to the chromosome of our genetic algorithm. It will be the class to modify if we want to modify the index to use in the fitness function of it.
* MainGAExecution: Class designed to perform tests with our implementation.
* Utils: Scala object that includes some helpful methods.
## Execution
If you only want to perform a test with the default configuration of our algorithm, the user must modify only the "MainGAExecution" class for it, more specifically the following parameters:
* origin: Set the path where the dataset is located.
* destination: Set the path to the saved destination for the result of the GA execution.
* dimension: Set the total columns of the used dataset.
* delimiter: Set the delimiter for each column into the dataset.
If, on the other hand, the user wishes to modify the configurations of the genetic algorithm established by default, he must modify the following classes according to the part he wishes to modify:
* GeneticAlgorithm: Within the main configurations of the genetic algorithm, the user can modify the following parameters if desired:
  * POPULATION_SIZE: Set the number of individuals within the algorithm population. By default, its value is 200 chromosomes.
  * NUM_GENERATIONS: Set the maximum number of generations the algorithm will produce. By default, its value is 100 iterations.
  * MUTATION_RATE: Set the probability for an individual to mutate during the generational change of each iteration. By default, its value is 10%. 
  * MUTATION_WEIGHTS: Set the probability for an individual to mutate the genes of the attributes during the generational change of each iteration. By default, its value is 70%.
  * MUTATION_K: Set the probability for an individual to mutate the gene from the number of clusters during the generational change of each iteration. By default, its value is 30%.
  * CROSSOVER_RATE: Set the percentage of an individual when he crosses with another during the generational change of each iteration. By default, its value is 95%.
  * NUM_ELIT_CHROMOSOMES: Set the number of elite chromosomes for each generational change. By default, its value is 2.
  * TOURNAMENT_SIZE: Set the number of chromosomes for the selection tournament of each generational change. By default, its value is 2.
  * K_MAX: Set the maximum number of clusters an individual can have within the generated population. By default, its value is 19, but it must be taken into account that this configuration will be between 2 and this value -1, that is, in this case between 2 and 18.
* Chromosome_Clustering: If, in addition to the main configurations of the genetic algorithm, the user wishes to modify the internal validation index for clustering used for the calculation of the fitness function in each individual, this class must be modified. To do this, you simply have to uncomment the line corresponding to the value desired by the user and comment on the one that was previously in use within the "recalculated fitness" method of this class.
## Result
By default, results of the GA execution are saved in the folder set in the "destination" parameter that contains part-00000 file. Within which are the winning individuals of each of the executed generations. In the format: "Generation #(Iteration)=>Fittestchromosome:[f=VALUE_FITNESS][CHROMOSOME]".
## Contributos
* José David Martín-Fernández.
* José C. Riquelme Santos.
* Beatriz Pontes Balanza.
