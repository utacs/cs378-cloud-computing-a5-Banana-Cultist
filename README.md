# Please add your team members' names here. 

## Team members' names 

1. Student Name: Aadit Barua

   Student UT EID: ab68445

2. Student Name: Daniel Lim

   Student UT EID: dwl667

3. Student Name: Bruce Fan

   Student UT EID: bxf64

4. Student Name: Kous Nyshadham

   Student UT EID: kn8794



##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 
YARN history
![Screenshot (48)](https://github.com/utacs/cs378-cloud-computing-a5-Banana-Cultist/assets/98183466/5f948159-4f30-4f38-b227-a6a7e91a1c4e)

Machines
![Screenshot (47)](https://github.com/utacs/cs378-cloud-computing-a5-Banana-Cultist/assets/98183466/051c5b70-f6d5-4f52-89a1-01375a9ce129)

Task1
![Screenshot (50)](https://github.com/utacs/cs378-cloud-computing-a5-Banana-Cultist/assets/98183466/f7b6d155-309f-4909-b021-04800ae27193)

Task2
![Screenshot (49)](https://github.com/utacs/cs378-cloud-computing-a5-Banana-Cultist/assets/98183466/2186e0b3-26e1-48fb-aa58-4bcf72f5e206)

Task3
![Screenshot (44)](https://github.com/utacs/cs378-cloud-computing-a5-Banana-Cultist/assets/98183466/2c53752c-5759-4774-ba75-4d38f1f040a4)


# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  taxi-data-sorted-large.csv

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  Task output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
