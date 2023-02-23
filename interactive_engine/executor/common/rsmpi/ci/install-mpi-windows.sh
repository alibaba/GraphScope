echo "Installing MS-MPI SDK..."

curl -o "msmpisdk.msi" -L "https://github.com/Microsoft/Microsoft-MPI/releases/download/v10.1.1/msmpisdk.msi"

msiexec.exe //i msmpisdk.msi //quiet //qn //log ./install.log

echo "Installed MS-MPI SDK!"

echo "Installing MS-MPI Redist..."

curl -o "msmpisetup.exe" -L "https://github.com/Microsoft/Microsoft-MPI/releases/download/v10.1.1/msmpisetup.exe"

./msmpisetup.exe -unattend -full

echo "Installed MS-MPI Redist!"
