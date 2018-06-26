import os
from neo4j.v1 import GraphDatabase
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import KDTree
from sklearn.metrics import roc_curve, auc

n4j_driver = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j', os.environ['NEO4J_AUTH'].split('/')[1]))

def shuffle_neighbors(gen0data, k=3):
    kdt = KDTree(gen0data)
    def get_nni(point):
        dist, ind = kdt.query([point], k=(k + 1))
        return np.random.choice(ind[0,1:])
    def shuffle_coords(p0, p1):
        result = np.array([p0, p1])
        for i in range(len(p0)):
            result[:, i] = np.random.choice(result[:, i], size=2, replace=False)
        return result
    gen1data = gen0data.copy()
    shuffled_indices = set()
    for i, p0 in enumerate(gen1data):
        if i in shuffled_indices: next
        nni = get_nni(p0)
        p1 = gen1data[nni]
        gen1data[i], gen1data[nni] = shuffle_coords(p0, p1)
        shuffled_indices.update([i, nni])
    return gen1data

def plot_roc_curve(y_test, y_score, save_fig_filename=None, fig_kwargs=None):
    # From http://scikit-learn.org/stable/auto_examples/model_selection/plot_roc.html#sphx-glr-auto-examples-model-selection-plot-roc-py
    # Compute ROC curve and ROC area for each class
    fpr, tpr, thresh = roc_curve(y_test, y_score)
    roc_auc = round(auc(fpr, tpr), 2)

    # Plot
    if fig_kwargs:
        plt.figure(**fig_kwargs)
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             lw=lw, label='ROC curve (area = {})'.format(roc_auc))
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC')
    plt.legend(loc="lower right")
    if save_fig_filename:
        plt.savefig(save_fig_filename, format='png')
    plt.show()
