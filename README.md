# Graph-Clustering 

# Data for shrinkage

For data shrinkage you should have listed files

1.  wiki-topcats.txt.gz	Hyperlink network of Wikipedia 

1.  wiki-topcats-categories.txt.gz	Which articles are in which of the top categories

1.  wiki-topcats-page-names.txt.gz	Names of the articles

You can download this data from https://snap.stanford.edu/data/wiki-topcats.html or from https://yadi.sk/d/1Uu4-MhFX-niqQ (faster way)

To run shrinkage:
>python3 src/data_separation.py \<arg1> \<arg2> \<arg3> \<arg4>
where:
*    arg1: path to adjacency list (wiki-topcats.txt)
*    arg2: path to list of description for each node (wiki-topcats-page-names.txt)
*    arg3: path for saving shrinked adjacency list (\<edges files>)
*    arg4: path for saving shrinked list of description for each node (\<nodes files>)

# Data after filteration
Can be found in the master branch in data/filtered-wikinodes5.txt and data/filtered-topcats5.txt

# User Manual

To run the Jar, use the following form:
>spark-submit Graph-Clustering.jar \<edges files> \<nodes files> \<Out Directory>


The output directory will contains the output file, with this format at each line 
> node_id,cluster_id


Same instructions applied to run the code from IDE.
