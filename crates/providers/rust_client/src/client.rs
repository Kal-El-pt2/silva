use std::env;
use anyhow::Result;
use chiral_client::file::FtpClient;

#[derive(Debug,Clone)]
pub struct RustClient {
    pub url: String,
    pub user_email: String,
    pub user_id: String,
    pub token_auth: String,
    pub token_api: String,
    pub ftp_addr: String,
    pub ftp_port: u16,
}

impl RustClient {
    pub fn new(
        url: String,
        user_email: String,
        user_id: String,
        token_auth: String,
        token_api: String,
        ftp_addr: String,
        ftp_port: u16,
    ) -> Self {
        
        Self {
            url,
            user_email,
            user_id,
            token_auth,
            token_api,
            ftp_addr,
            ftp_port,
        }
    }


    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error >> {
        dotenvy::from_filename(".env").ok();
        let url = env::var("URL")?;
        let user_email = env::var("USER_EMAIL")?;
        let user_id = env::var("USER_ID")?;
        let token_auth = env::var("TOKEN_AUTH")?;
        let mut client = chiral_client::create_client(&url).await?;

        let response = chiral_client::get_token_api(&mut client, &user_email, &token_auth).await?;
        let token_api = response.as_str().ok_or("Expected a string in token API response")?.to_string();
        let ftp_addr = env::var("FTP_ADDR")?;
        let ftp_port = env::var("FTP_PORT")?.parse::<u16>()?;

        Ok(Self::new(
            url,
            user_email,
            user_id,
            token_auth,
            token_api,
            ftp_addr,
            ftp_port,
        ))
    }



    pub async fn get_credits(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::get_credit_points(&mut client, &self.user_email, &self.token_auth).await
    }

    pub async fn submit_job(&mut self, command_string: &str, project_name: &str, input_files: &[&str], output_files: &[&str]) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;        
        chiral_client::submit_job(
            &mut client,
            &self.user_email, 
            &self.token_auth, 
            command_string, 
            project_name, 
            input_files, 
            output_files
        ).await
    }

    pub async fn get_job(&mut self, job_id: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::get_job(&mut client, &self.user_email, &self.token_auth, job_id).await
    }

    pub async fn list_projects(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;

        chiral_client::list_of_projects(&mut client, &self.user_email, &self.token_auth).await
    }

    pub async fn list_example_projects(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::list_of_example_projects(&mut client,&self.user_email,&self.token_auth).await
    }

    pub async fn get_project_files(&mut self,project_name: &str,file_name:&str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::get_project_files(&mut client, &self.user_email, &self.token_auth,project_name,file_name).await
    }

    pub async fn list_project_files(&mut self, project_id: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::list_of_project_files(&mut client, &self.user_email, &self.token_auth, project_id).await
    }

    pub async fn import_example_project(&mut self, project_name: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::import_example_project(&mut client, &self.user_email, &self.token_auth, project_name).await
    }

    pub async fn get_api_token(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::get_token_api(&mut client, &self.user_email, &self.token_auth).await
    }

    pub async fn refresh_api_token(&mut self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut client = chiral_client::create_client(&self.url).await?;
        chiral_client::refresh_token_api(&mut client,&self.user_email ,&self.token_api).await
    }

    pub async fn make_directory(&mut self, dir_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut ftp_client = FtpClient::new(
            &self.ftp_addr,
            self.ftp_port,
            &self.user_email,
            &self.token_api,  // Changed from token_auth to token_api
            &self.user_id,
        );
        
        ftp_client.connect()?;
        ftp_client.make_directory(dir_name)?;
        ftp_client.disconnect();
        
        Ok(())
    }

    pub async fn upload_file(&mut self, local_path: &str, remote_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut ftp_client = FtpClient::new(
            &self.ftp_addr,
            self.ftp_port,
            &self.user_email,
            &self.token_api,  // Changed from token_auth to token_api
            &self.user_id,
        );
        
        ftp_client.connect()?;
        ftp_client.upload_file(local_path, remote_path)?;
        ftp_client.disconnect();
        
        Ok(())
    }

    pub async fn download_file(&mut self, remote_path: &str, local_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut ftp_client = FtpClient::new(
            &self.ftp_addr,
            self.ftp_port,
            &self.user_email,
            &self.token_api,  // Changed from token_auth to token_api
            &self.user_id,
        );
        
        ftp_client.connect()?;
        ftp_client.download_file(remote_path, local_path)?;
        ftp_client.disconnect();
        
        Ok(())
    }
    // Test the connection
    pub async fn test_connection(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.get_credits().await?;
        println!("Connection test successful!");
        Ok(())
    }
}