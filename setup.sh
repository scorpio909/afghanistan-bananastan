#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Setting up the environment for Arcanus Prime..."
echo "Creating Python virtual environment..."
python3 -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing required Python packages..."
echo "---------------------------------------------------------------------"
echo "Regarding CuPy installation:"
echo "The following command attempts to install CuPy for CUDA 11.x (cupy-cuda11x)."
echo "Please ensure this matches your installed NVIDIA CUDA version."
echo "Common options include: cupy-cuda11x (for CUDA 11.0-11.8), cupy-cuda12x (for CUDA 12.x)."
echo "If you have a different CUDA version, or want the latest compatible CuPy,"
echo "please visit the CuPy installation guide: https://docs.cupy.dev/en/stable/install.html"
echo "If you don't have an NVIDIA GPU or CUDA installed, you can omit 'cupy-cuda11x'"
echo "from the pip install command for CPU-only operation (Pandas UDFs will use NumPy)."
echo "---------------------------------------------------------------------"

# Install Python packages. Modify cupy-cuda11x as needed (see message above).
pip install pyspark==3.5.0 delta-spark==3.2.0 numpy pandas pyarrow zstandard cupy-cuda11x

echo "---------------------------------------------------------------------"
echo "Python packages installed."
echo ""
echo "Environment 'venv' is ready."
echo "Activate it in your shell by running: source venv/bin/activate"
echo ""
echo "IMPORTANT NEXT STEPS:"
echo "1. Ensure Apache Spark is installed and SPARK_HOME is configured in your environment."
echo "2. For true GPU acceleration with NVIDIA RAPIDS:"
echo "   - Ensure NVIDIA CUDA drivers are correctly installed and accessible."
echo "   - Download the RAPIDS Accelerator for Apache Spark JAR compatible with your Spark version (3.5.0 here)"
echo "     and Scala version (usually 2.12 for Spark 3.x)."
echo "   - Place the JAR in a known location."
echo "   - You will need to pass this JAR path to spark-submit via the --jars option."
echo "     (e.g., --jars /path/to/rapids-4-spark_2.12-YOUR_VERSION.jar)"
echo "3. Confirm your CuPy installation (e.g., cupy-cuda11x) matches your CUDA version on ALL Spark cluster nodes if applicable."
echo "---------------------------------------------------------------------"
