### Environment

python -m venv .venv
.\.venv\Scripts\Activate.ps1

### Dependencies

pip install -r .\requiements.txt

uvicorn app.main:app --reload


## Deploy AWs EC2

sudo yum update
sudo yum install git -y
sudo yum install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

exit

git clone https://github.com/Tavo826/business-management-stock

cd business-management-stock

nano.env


docker build -t rag-service .

docker run -d --name rag-service --restart unless-stopped --env-file .env -p 8080:8080 -v /home/ec2-user/rag-data:/app/data rag-service

