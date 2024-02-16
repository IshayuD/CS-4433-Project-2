import csv
import random

# seed
random.seed(0)

number_of_points = 3000
x_min = 0
x_max = 5000
y_min = 0
y_max = 5000
points_list = []
output_pathfile_points = 'data_points.csv'
output_pathfile_k_centroids = 'k_centroids.csv'
# generate points
for i in range(number_of_points):
    x = random.randint(x_min, x_max)
    y = random.randint(y_min, y_max)
    print(f"Point {i}: ({x},{y})")
    points_list.append((x, y))
    with open(output_pathfile_points, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['x', 'y'])
        writer.writerows(points_list)

print(f"Points successfully written to file '{output_pathfile_points}'\n\n")

# generate centroids
k = 3
centroid_list = []
for i in range(k):
    x = random.randint(x_min, x_max)
    y = random.randint(y_min, y_max)
    print(f"Centroid #1: ({x},{y})")
    centroid_list.append((x, y))
    with open(output_pathfile_k_centroids, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['x', 'y'])
        writer.writerows(centroid_list)
print(f"Points successfully written to file '{output_pathfile_k_centroids}'")