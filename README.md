# CompatibilityBasedRoleMatching

Code Repository for the Submission "Role Matching in Temporal Data " to VLDB 2022.

# Abstract

We present role matchings, a novel, fine-grained integrity constraint on temporal fact data, i.e., ⟨subject, predicate, object, timestamp⟩-quadruples. A role is a combination of subject and predicate and can be associated with different objects as the real world evolves and the data changes over time. A role matching is a novel constraint that states that  the associated object of two or more different roles should always match at the same timestamps. Once discovered, role matchings can serve as integrity constraints that, if  violated, can alert editors and thus allow them to correct the error. We present compatibility-based role matching (CBRM), an algorithm to discover role matchings in large datasets, based on their change histories.

We evaluate our method on datasets from the Socrata open government data portal, as well as Wikipedia infoboxes, showing that our approach can process large datasets of up to  3.5 million roles containing up to 17 million changes. Our approach consistently outperforms baselines, achieving almost 30 percentage points more F-Measure on average

# Usage

The repository contains several Main classes, that can start different parts of the Compatibility Based Role Matching by themselves. There is typically an executable .sh script for every Main class that will execute the experiments that we report on in the paper:


* [CompatibilityGraphCreationMain](src/main/scala/de/hpi/role_matching/cbrm/compatibility_graph/role_tree/CompatibilityGraphCreationMain.scala) - Creation of the unweighted compatiblity graph from a set of vertices (also see [here](src/main/executable/compatibility_graph_creation.sh) for an executable linux shell script that repeats our expermients as presented in the paper)
* [TuningDataExport](src/main/scala/de/hpi/role_matching/cbrm/evidence_based_weighting/TuningDataExportMain.scala) - Transforms the simple but memory inefficient graph format to a more compressed version and exports statistics (as csv) which are used for the tuning of the evidence based weighting. Also see [here](src/main/executable/tuningDataExport.sh) for an executable linux shell script that runs this Main class and exports the data used in our experiments as presented in the paper.
* [SparseGraphCliquePartitioningMain](src/main/scala/de/hpi/role_matching/cbrm/sgcp/SparseGraphCliquePartitioningMain.scala) - Executes the sparse graph clique partitioning. Also see [here](src/main/executable/sgcp.sh) for an executable linux shell script that runs this Main class.
