const express = require("express");
const app = express();
const cors = require("cors");
const {Pool} = require("pg");
const {createClient} = require("redis");
const bull = require("bull");

app.use(express.json());
app.use(cors());

const pg = new Pool({
    connectionString: "postgres://postgres:qwerty7867@localhost:5432/twitter-feed"
});

const redisClient = createClient({
    url: "redis://localhost:6379"
});

(async()=>{
    await redisClient.connect();
    console.log("Redis Client Connected");
})()

redisClient.on("error" , (err)=>{
    console.log("Redis Client Error : " + err.message); 
})

const feedQueue = new bull("feed-queue", {
   redis : {
    host: "localhost",
    port: 6379
   }
});


app.post("/initialize-table", async(req, res)=>{
    try {
        await pg.query(`CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE,
            followers INTEGER DEFAULT 0, following INTEGER DEFAULT 0)`);
        await pg.query(`CREATE TABLE IF NOT EXISTS tweets (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id), content VARCHAR(280))`);
        await pg.query(`CREATE TABLE IF NOT EXISTS follows (id SERIAL PRIMARY KEY, follower_id INTEGER REFERENCES users(id), followed_id INTEGER REFERENCES users(id))`);

        res.json({message: "Tables initialized successfully"});
    } catch(err){
        res.status(500).json({error: "Internal Server Error : " + err.message});
    }
})

app.post("/users" , async(req , res)=>{
    try {
           const {name, email} = req.body;
           const result = await pg.query("INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *", [name, email]);
           res.json(result.rows[0]);
           
    } catch (error) {
        res.status(500).json({error: "Internal Server Error : " + error.message});
    }
});

app.get("/users", async(req, res)=>{
    try {
        const result = await pg.query("SELECT * FROM users");
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({error: "Internal Server Error : " + error.message}); 
    } 
})

app.post("/follow-unfollow", async(req, res)=>{
    const client = await pg.connect();
    try {
        const {follower_id, followed_id} = req.body;
        let operation = "follow";

        if(follower_id === followed_id){
            res.status(400).json({error: "Cannot follow yourself"});
            return; 
        }
        const followerResult = await client.query("SELECT * FROM users WHERE id = $1", [follower_id]);
        const followedResult = await client.query("SELECT * FROM users WHERE id = $1", [followed_id]);

        if(followerResult.rows.length === 0 || followedResult.rows.length === 0){
            res.status(404).json({error: "User not found"});
            return;  
        }
        
        await client.query("BEGIN");
        const result = await client.query("SELECT id FROM follows WHERE follower_id = $1 AND followed_id = $2", [follower_id, followed_id]);

         if(result.rows.length > 0){
            await client.query("DELETE FROM follows WHERE follower_id = $1 AND followed_id = $2", [follower_id, followed_id]);
            await client.query("UPDATE users SET followers = followers - 1 WHERE id = $1", [followed_id]);
            await client.query("UPDATE users SET following = following - 1 WHERE id = $1", [follower_id]);
            operation = "unfollow";
         } else {
            await client.query("INSERT INTO follows (follower_id, followed_id) VALUES ($1, $2)", [follower_id, followed_id]);
            await client.query("UPDATE users SET followers = followers + 1 WHERE id = $1", [followed_id]);
            await client.query("UPDATE users SET following = following + 1 WHERE id = $1", [follower_id]);
         }
         await client.query("COMMIT");
         res.json({message: `Successfully ${operation}ed user`});

    } catch (error) {
        await client.query("ROLLBACK");
        res.status(500).json({error: "Internal Server Error : " + error.message}); 
    } finally{
        client.release();
    }
});

app.post("/tweet", async(req, res)=>{
    try {

         const {user_id, content} = req.body;
         const userResult = await pg.query("SELECT * FROM users WHERE id = $1", [user_id]);
         if(userResult.rows.length === 0){
            res.status(404).json({error: "User not found"});
            return;
         }
         const result = await pg.query("INSERT INTO tweets (user_id, content) VALUES ($1, $2) RETURNING *", [user_id, content]);
         console.log(result.rows[0]);

         feedQueue.add({tweet_id : result.rows?.[0]?.id ,  content , user_id : user_id} , {
            removeOnComplete:true,
            delay: 0
         });
          
         res.json({message: "Tweet added"});

    } catch (error) {
          res.status(500).json({error: "Internal Server Error : " + error.message}); 
    } 
})

feedQueue.process(async(job)=>{
    try {
          console.log("----------------------------------------------");
          const data = await job.data;
          console.log("Processing job : " + JSON.stringify(data));

          const followersResult = await pg.query("SELECT follower_id FROM follows WHERE followed_id = $1", [data.user_id]);
          if(followersResult.rows.length > 0){
            const followers = followersResult.rows.map((row)=>row.follower_id);

            const feedData = {
                tweet_id: data.tweet_id,
                user_id: data.user_id,
                content: data.content
            };

            for( const follower_id of followers){
                const key = `feed:${follower_id}`;
                const existingFeed = await redisClient.get(key);
                if(existingFeed){
                    console.log("Existing Feed in Redis :-------- ");
                    const feed = JSON.parse(existingFeed);
                    feed.push(feedData);
                    await redisClient.set(key, JSON.stringify(feed) , {
                        expiration: {
                            type : "EX" ,
                            value: 300
                        }
                    }); 
                } else {
                    console.log("New Feed in Redis :-------- ");
                    await redisClient.set(key, JSON.stringify([feedData])); 
                }
            };

          }
    } catch (error) {
        console.log("Error processing job : " + error.message); 
    } 
})

app.get("/feed/:user_id", async(req, res)=>{
    try {
        const {user_id} = req.params;
        const userResult = await pg.query("SELECT * FROM users WHERE id = $1", [user_id]);
        if(userResult.rows.length === 0){
            res.status(404).json({error: "User not found"});
            return;
        }

        const key = `feed:${user_id}`;
        const existingFeed = await redisClient.get(key);
        if(existingFeed){
            const feed = JSON.parse(existingFeed);
            res.json({feed , message : "From Redis"}); 
        } else {
           const result = await pg.query(`
            SELECT tweets.id as tweet_id, tweets.user_id as user_id, tweets.content as content FROM tweets
            JOIN follows ON tweets.user_id = follows.followed_id
            WHERE follows.follower_id = $1` , [user_id]); 
            const feed = result.rows;
            await redisClient.set(key, JSON.stringify(feed) , {
                expiration: {
                    type : "EX" ,
                    value: 300
                }
            });
            res.json({feed , message : "From Database"});
        }

    } catch (error) {
        res.status(500).json({error: "Internal Server Error : " + error.message}); 
    }
})


const port = 3000;
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});