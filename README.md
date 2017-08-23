This repository contains code demoing a project, *Tolerant retrieval of Finnish text for language learners using finite-state error modeling*. At the moment is it still at proof of concept level and probably not of much interest to others (yet).

Regardless, here's the directory layout:
/fst - Python scripts to build a transducer acting as an error model of English speakers transcribing Finnish.
/engine - Code for a testbed written in Rust.
/evaluation - Evaluation data, results and Python scripts to create plots.
/testset - Code to generate good test words.
