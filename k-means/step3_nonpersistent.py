"""Calculates the k-means centers and clusters
INSTRUCTIONS:

1) SSH into master node of the EMR cluster:
    ~$ ssh -i key.pem hadoop@ec2-X-XX-XXX-XX.compute-1.amazonaws.com

2) Navigate to directory containing step3_nonpersistent.py

3) Enter into terminal as follows:
    ~$ spark-submit step3_nonpersistent.py s3://bucket-name/filename s3://bucket-name/folder distance_function k max_iterations
        spark-submit: command
        step3_nonpersistent.py: the python application
        s3://bucket-name/filename: Path to input file, change names as appropriate
        s3://bucket-name/directory: Directory for output (note final output directory can not already exist)
        distance_function: 'circle' or 'euclid' to define the distance calculation
        k: integer indicating number of clusters
        max_iterations: integer indicating the desired max number of iterations (generally not reached so high number is fine)
    EXAMPLE: ~$ spark-submit step3_nonpersistent.py s3a://final-kmeans/clean/mobilenet.csv s3://final-kmeans/results circle 5 50

"""


## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from math import asin, sin, cos, sqrt, pi
from random import sample
from operator import add
import sys

## Constants
APP_NAME = "K_means"

def main(sc,filename,outpath,dist_func,k=4,maxIters=30):
    spark = SQLContext(sc)
    # Parse lat and long into points
    def parse_points(filepath):
        # Read in file
        lines = sc.textFile(filepath)
        # Split each line by "," - for csv files
        parts = lines.map(lambda l: l.split(','))
        points = parts.map(lambda p: (float(p[0]),float(p[1])))

        return points

    # closestPoint: given a (latitude/longitude) point and an array of current center points,
    # returns the index in the array of the center closest to the given point
    def closestPoint(point, centers): # point is a tuple, centers is an array of tuples
        mindist = None
        closest = 0
        for i in range(k):
            dist = dist_func(point, centers[i])
            if mindist == None:
                mindist = dist
            if dist < mindist:
                mindist = dist
                closest = i
        return closest

    # addPoints: given two points, return a point which is the sum of the two points.
    def addPoints(pt1, pt2): # points are tuples
        return (pt1[0]+pt2[0],pt1[1]+pt2[1])

    # EuclideanDistance: given two points, returns the Euclidean distance of the two.
    def EuclideanDistance(pt1, pt2): # points are tuples
        lat1 = float(pt1[0])
        lat2 = float(pt2[0])
        lon1 = float(pt1[1])
        lon2 = float(pt2[1])
        dist = ((lat1-lat2)**2 + (lon1-lon2)**2)**.5
        return dist

    # GreatCircleDistance: given two points, returns the great circle distance of the two.
    def GreatCircleDistance(pt1, pt2): # points are tuples
        r = 6371 # Earth mean radius in km
        lat1 = float(pt1[0])*pi/180
        lat2 = float(pt2[0])*pi/180
        lon1 = float(pt1[1])*pi/180
        lon2 = float(pt2[1])*pi/180
        dist=2*asin(sqrt((sin((lat1-lat2)/2))**2 + cos(lat1)*cos(lat2)*(sin((lon1-lon2)/2))**2))*r
        return dist

    def get_centers(pt_sums, num_pts): #num_points is a dictionary with index as key
        centers = []
        for i in range(k):
            lat = pt_sums[i][0] / num_pts[i]
            lon = pt_sums[i][1] / num_pts[i]
            centers.append((lat,lon))
        return centers

    def init_centers(points,k):
        # sample(points,k)
        return points.takeSample(False,k)


    def k_means(points, centers=None, iters=0):
        if centers == None:
            centers = init_centers(points,k)
        # Get closest center for each point as key
        clusters = points.map(lambda pt: (closestPoint(pt,centers),pt))
        # Get sum and count of points in each cluster
        pts_per_cluster = clusters.map(lambda pts: (pts[0],1)).reduceByKey(lambda x,y: x+y)
        cluster_sum = clusters.reduceByKey(lambda pt1,pt2: (addPoints(pt1,pt2))) # make cluster number the key???
        # Get new centers by dividing sum of points by number of points in cluster

        pt_sums = dict(cluster_sum.collect())
        num_pts = dict(pts_per_cluster.collect())
        new_centers = get_centers(pt_sums,num_pts)

        def clustersToCSV(line):
            return ','.join((str(line[0]),str(line[1][0]),str(line[1][1])))

        def centersToCSV(centers):
            spark.createDataFrame(centers).coalesce(1)\
                .write.csv(outpath+'/'+out_folder+'/centers_'+'k'+str(k)+'_'+save_dist)


        for i in range(k):
            # if a distance in the new centers is greater than convergeDist - iterate again
            centers_dist = dist_func(new_centers[i],centers[i])
            if centers_dist > convergeDist:
                break
            # if the loop completes then the algorithm has converged
            if i == k-1:
                print('Completed')
                print('TOTAL ITERATIONS: ',iters)
                lines = clusters.map(clustersToCSV)
                lines.coalesce(1).saveAsTextFile(outpath+'/'+out_folder+'/clusters_'+'k'+str(k)+'_'+save_dist)
                print('Cluster Information Saved')
                print('Center Points:')
                print(new_centers)
                centersToCSV(new_centers)
                return new_centers
        if iters == maxIters:
            print('REACHED MAX ITERATIONS:',iters)
            lines = clusters.map(clustersToCSV)
            lines.coalesce(1).saveAsTextFile(outpath+'/'+out_folder+'/clusters_'+'k'+str(k)+'_'+save_dist)
            print('Cluster Information Saved')
            print('Center Points:')
            print(new_centers)
            centersToCSV(new_centers)
            return new_centers
        k_means(points,new_centers, iters=iters+1)

    # Convert the string input to the proper function
    if 'euclid' in dist_func.lower():
        dist_func = EuclideanDistance
        convergeDist = 1 # distance in lat/long
        save_dist = 'euc'
    elif 'circle' in dist_func.lower():
        dist_func = GreatCircleDistance
        convergeDist = 25 # km
        save_dist = 'circ'

    # Get list of points
    points = parse_points(filename)

    out_folder = filename.split('/')[-1][:-4]

    return k_means(points)

if __name__ == "__main__":

    # spark-submit final_test.py ./test/sample_geo.csv ./test/clusters.csv circle 3 30

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   filename = sys.argv[1]
   outpath = sys.argv[2]
   dist_func = sys.argv[3]
   k = int(sys.argv[4])
   maxIters = int(sys.argv[5])
   # Execute Main functionality
   main(sc,filename,outpath,dist_func,k,maxIters)
