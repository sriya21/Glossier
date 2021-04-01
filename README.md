# Glossier Take Home assessment 
Requirements:
    python 3.7
    pandas 0.25.3
    sqlalchemy 1.3.16
    requests
    schedule
    time
    json

Run:
1.Please install the requirements to test the project
2.Main function has commented lines - uncomment it to test it immediately and comment the scheduler with infinite loop

Steps involved:
1.The project read the zip file from S3 and extract it combine it into a single JSON data frame
2.Normalize the data to store it in postgres
3.To avoid redundancy and to reduce the storage used. Items are created as a separate table with order_id(id) as a 
  foreign key to the order table.so that using join we can still fetch the actual data.
4.Created user table with average money a user spent with number of orders he made could be useful for the data science 
  team to estimate the customer priority
5.Product table is created with how many items are sold in each product.
