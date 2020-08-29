-The core scala source code is availabe at src/main/scala-2.11
-The datasink algorithm is broken into 4 phases
	-Phase 1:FeatureEngineering.scala
	-Phase II:Modeling
		-PhaseIIModelingBase.scala
		-PhaseIIModelingOuter.scala
	-Phase III-Stacking Model: PhaseIIICombination.scala
	-Model Evaluation:PhaseIVEvaluation.scala

-compile source code into java archive file
    -sbt clean package
    -the output jar file is located at target/scala-2.11
    -use the jar file in python script to submit spark job
-To facilitates the job parallelization; we have wrritten python script. It is available src/main/python/sparkSubmitYarn.py
-Please submit your spark job using the python scripts


