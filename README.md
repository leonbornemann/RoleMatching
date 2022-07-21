# CompatibilityBasedRoleMatching

Code Repository for the Submission "Role Matching in Temporal Data " to SIGMOD 2023.

# Usage

The repository contains several Main classes. Typically, every Main class that will execute parts of an experiment that we report on in the paper:


* [CBRBMain](src/main/scala/de/hpi/role_matching/blocking/cbrb/CBRBMain.scala) - Execution of the CBRB Blocking Method (with the option of applying decay beforehand)
* [PrintBlockingResultSetSizesMain](src/main/scala/de/hpi/role_matching/blocking/group_by_blockers/PrintBlockingResultSetSizesMain.scala) - Executes all blocking methods that can be implemented through a simple group-by (EM, CQM, TSM, VSM) and reports their statistics.
* [RoleDomainExportMain](src/main/scala/de/hpi/role_matching/blocking/rm/RoleDomainExportMain.scala) - Exports data for the implementation of RM (Link available soon).
* [LabelledRoleMatchingEvaluation](src/main/scala/de/hpi/role_matching/evaluation/blocking/ground_truth/LabelledRoleMatchingEvaluation.scala) - generates CSV-Files for role match candidates for the graphical Evaluation (Jupyter Notebooks)
* [AllPairSamplerMain](src/main/scala/de/hpi/role_matching/evaluation/blocking/sampling/AllPairSamplerMain.scala) - Samples candidates from the set of all pairs
* [TVA_DVA_SamplerMain](src/main/scala/de/hpi/role_matching/evaluation/blocking/sampling/TVA_DVA_SamplerMain.scala) - Samples candidates from a set restricted on TVA and DVA (95TVA and 2DVA for the results in the paper)
* [DittoExporterFromRMBlockingMain](src/main/scala/de/hpi/role_matching/matching/DittoExporterFromRMBlockingMain.scala) - Export (serialize) role match candidates from the RM blocking to the expected input format for Ditto
* [DittoResultToCSVMain](src/main/scala/de/hpi/role_matching/evaluation/matching/DittoResultToCSVMain.scala) - Export Results from the Matching via Ditto as CSV for the graphical evaluation (Jupyter Notebooks)
* [RolesetStatisticsPrintMain](src/main/scala/de/hpi/role_matching/evaluation/RolesetStatisticsPrintMain.scala) - Print general statistics for every dataset 
