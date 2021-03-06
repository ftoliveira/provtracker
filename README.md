## Welcome to ProvTracker

## Towards effective reproducible botnet detection methods through scientific workflow management systems

Even after nearly two decades of the creation of the first botnet, the detection and mitigation of their attacks remain one of the biggest challenges faced by researchers and cyber-security professionals. Although there are numerous studies related to botnet detection, estimate how much one method is better than another is still an open problem, mainly because of the difficulty
in comparing and reproducing such methods. This work proposes an architecture, implemented with SPARK as a high-performance data processing solution, together with VISTRAILS as a workflow management and data provenance solution, to address this comparability and reproducibility problem in a large-scale environment, as well as a tool, ProvTracker, to analyze and compare the methods
results.

Here you can download ProvTracker and test it with the provenance captured from our workflows.

We've tested with python >= 2.7.10 and pip installed.

To install it, just download the ZIP file, unzip it and execute the commands bellow inside the directory just created:

```markdown

# First install the requirements
pip install -r requirements.txt

## Then start the http server
python ProvTracker.py

### The HTTP server will be started on port 5000
On the browser, point to: http://localhost:5000

```

We also provide some workflows examples and the Vistrails' modules developed.

```markdown

# First
Download and install Vistrails: www.vistrails.org

# Second
Copy the folder "spark" to vistrails/packages directory

# Third
Open an example from Vistrails

# You should install the paramiko python module in order to use this tool

```

## In order to run the examples, it's necessary to start a Spark cluster.

## Soon, we will release a virtual machine image will all our experiment.

Enjoy it!

