use std::sync::{Arc, Mutex};
use std::time::Duration;
use ratatui::prelude::*;
use ratatui::widgets::*;
use sacloud_rs::api::dok;
use rust_client::{self, RustClient};
use futures_util::stream::StreamExt;
use anyhow::{Result, anyhow};
use crate::data_model;
use crate::ui;
use crate::utils;
use serde_json::Value;
use crate::data_model::job::settings::Settings;
use std::path::Path;

pub const HELPER: &[&str] = &[
    "Launch a job", 
];

async fn launch_job_local(
    job_mgr: Arc<Mutex<data_model::job::Manager>>,
    job_id_to_cancel: Arc<Mutex<Option<usize>>>,
    proj_dir: std::path::PathBuf,
    settings_local: data_model::provider::local::Settings, 
) -> anyhow::Result<()> {
    // TODO: currently only support one job
    let job_id = 0;
    let working_dir = "/workspace";

    let volume_binds = vec![
        format!("{}:{}", proj_dir.to_str().unwrap(), working_dir)
    ];
    let docker = bollard::Docker::connect_with_socket_defaults().unwrap();
    let container_id = utils::docker::launch_container(&docker, &settings_local.docker_image, volume_binds).await?;
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[Local infra] exec job {job_id}, status: using image {}", settings_local.docker_image));
        job_mgr.add_log(job_id, format!("[Local infra] exec job {job_id}, status: container {container_id} created"));
        let mut job = data_model::job::Job::new(job_id);
        job.infra = data_model::job::Infra::Local;
        let _ = job_mgr.jobs.insert(job_id, job);
    }

    let exec_id = docker.create_exec(
        &container_id,
        bollard::models::ExecConfig {
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            cmd: Some(
                vec!["sh", &settings_local.script]
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ),
            working_dir: Some(working_dir.to_string()),
            ..Default::default()
        }
    ).await?
    .id;

    if let bollard::exec::StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec_id, None).await? {
        loop {
            let cancel_job = {
                let job_id_to_cancel = job_id_to_cancel.lock().unwrap();
                if let Some(id_cancel) = *job_id_to_cancel {
                    id_cancel == job_id
                } else {
                    false
                }
            };

            if cancel_job {
                let scob = bollard::query_parameters::StopContainerOptionsBuilder::default();
                docker.stop_container(&container_id, Some(scob.signal("SIGINT").t(3).build())).await?;

                let mut job_mgr = job_mgr.lock().unwrap();
                job_mgr.add_log(job_id, format!("[Local infra] exec job {job_id}, container {container_id} stopped"));
                job_mgr.local_infra_cancel_job = false;
                break;
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if let Some(Ok(msg)) = output.next().await {
                let mut job_mgr = job_mgr.lock().unwrap();
                job_mgr.add_log(job_id, format!("[Local infra] exec job {job_id}, output: {msg}"));
            }
        }
    } else {
        unreachable!();
    }

    let rcob = bollard::query_parameters::RemoveContainerOptionsBuilder::default();
    docker.remove_container(&container_id, Some(rcob.force(true).build())).await.unwrap();
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[Local infra] exec job {job_id}, container {container_id} removed"));
    }

    Ok(())
}

async fn launch_job_dok(
    proj: data_model::project::Project, 
    registry: data_model::registry::Registry,
    client: sacloud_rs::Client,
    param_dok: dok::params::Container, 
    job_mgr: Arc<Mutex<data_model::job::Manager>>,
    with_build: bool,
) -> anyhow::Result<()> {
    // TODO: currently only support one job
    let job_id = 0;

    if with_build {
        // build & push the docker image
        utils::docker::build_image(&registry, &proj, job_mgr.clone()).await?;
        utils::docker::push_image(&registry, &proj, job_mgr.clone()).await?;
    } else {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, "Use docker image directly, no image will be built and pushed.".to_string());
        job_mgr.add_log(job_id, "All docker building parameters in @job.toml will be ignored".to_string());
    }

    // create the task
    let task_created = dok::shortcuts::create_task(client.clone(), param_dok).await?;
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[sakura internet DOK] task {} created", task_created.id));
        let mut job = data_model::job::Job::new(job_id);
        job.infra = data_model::job::Infra::SakuraInternetDOK(task_created.id.to_string(), None);
        let _ = job_mgr.jobs.insert(job_id, job);
    }

    // check task status
    let task = loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let task = dok::shortcuts::get_task(client.clone(), &task_created.id).await?;
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log_tmp(job_id, format!("[sakura internet DOK] task {} status: {}", task.id, task.status));
        if task.status == "done" {
            job_mgr.clear_log_tmp(&job_id);
            break task;
        }
        if let Some(http_uri) = task.http_uri.as_ref() {
            if let Some(job) = job_mgr.jobs.get_mut(&job_id) {
                job.infra = data_model::job::Infra::SakuraInternetDOK(task.id.to_string(), Some(http_uri.to_string()));
            }
        }
        if let Some(container) = task.containers.first() {
            if let Some(start_at) = &container.start_at {
                job_mgr.add_log_tmp(job_id, format!("[sakura internet DOK] task {} started at {}", task.id, start_at));
            } else {
                job_mgr.add_log_tmp(job_id, format!("[sakura internet DOK] task {} not ready for use", task.id));
            }
        }
    };

    // get artifact url
    let mut count = 0;
    let af_url = loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        count += 1;
        match dok::shortcuts::get_artifact_download_url(client.clone(), &task).await {
            Ok(af_url) => {
                let mut job_mgr = job_mgr.lock().unwrap();
                job_mgr.clear_log_tmp(&job_id);
                break af_url;
            }
            Err(_e) => {
                let mut job_mgr = job_mgr.lock().unwrap();
                job_mgr.add_log_tmp(job_id, 
                    format!("[sakura internet DOK] output files (artifact {}) of task {} not ready {}",
                        task.artifact.as_ref().unwrap().id, task_created.id, ".".repeat(count % 5))
                );
            }
        }
    };

    // download outputs  
    let filepath = proj.get_dir().join("artifact.tar.gz");
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[sakura internet DOK] downloading output files of task {}", task_created.id));
    }
    utils::file::download(&af_url.url, &filepath).await?;
    utils::file::extract_tar_gz(&filepath, proj.get_dir())?;
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[sakura internet DOK] downloaded output files of task {}", task_created.id));
    }
    std::fs::remove_file(&filepath)?;

    Ok(())
}


async fn launch_job_rust_client(
    proj: data_model::project::Project,
    mut rust_client: rust_client::RustClient,
    job_mgr: Arc<Mutex<data_model::job::Manager>>,
) -> Result<()> {
    let job_id = 0;

    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Launch Job started \n"));
    }

    let project_name = proj.get_project_name()?;

    let mut job = data_model::job::Job::new(job_id);
    job.infra = data_model::job::Infra::RustClient(
        format!("pending-{}", job_id),
        rust_client.url.clone(),
    );
    job_mgr.lock().unwrap().jobs.insert(job_id, job);

    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Job Created \n"));
    }

    let base_dir = proj.get_dir();
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Base dir: {:?} \n", base_dir));
    }
    let settings_path = base_dir.join("@job.toml");
    let settings = Settings::new_from_file(&settings_path)?;


    let input_files: Vec<&str> = settings.files.inputs.iter()
        .map(|filename| std::path::Path::new(filename).file_name().unwrap().to_str().unwrap())
        .collect();

    let output_files: Vec<&str> = settings.files.outputs.iter()
        .map(|filename| std::path::Path::new(filename).file_name().unwrap().to_str().unwrap())
        .collect();

    let script_files: Vec<&str> = settings.files.scripts.iter()
        .map(|filename| std::path::Path::new(filename).file_name().unwrap().to_str().unwrap())
        .collect();

    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] File Vectors Created \n"));
    }

    let _ = rust_client.connect_ftp().await;
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] FTP Connection Started \n"));
    }
    let job_dir = project_name.replace("_", "-");

    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Making Job Dir: {:?} \n", job_dir));
    }
    match rust_client.make_directory(&job_dir).await {
        Ok(_) => {}
        Err(e) => {
            let err_str = format!("{}", e);
            if !(err_str.contains("550") || err_str.contains("File exists")) {
                let mut job_mgr = job_mgr.lock().unwrap();
                job_mgr.add_log(job_id, format!("[RustClient Node] Job Dir already exists "));
                return Err(anyhow!("Failed to create remote directory '{}': {}", job_dir, err_str));
            }
        }
    }
    {
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Dir made\n" ));
    }
    
    let mut staged_inputs: Vec<&str> = Vec::new();

    staged_inputs.extend(input_files.iter().copied());
    staged_inputs.extend(script_files.iter().copied());

    let _ = rust_client.change_directory(&job_dir).await;
    {
        let changed_to = rust_client.current_directory().await.map_err(|e| anyhow::anyhow!("Failed to get current directory: {}", e))?;
        let mut job_mgr = job_mgr.lock().unwrap();
        job_mgr.add_log(job_id, format!("[RustClient Node] Dir Changed to : {:?} \n", changed_to  ));
    }
    let curr_dir = rust_client.current_directory().await
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {}", e))?;


    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload of Input and Script Files \n", )); 
    }

    for input_file in &staged_inputs {
        let local_path = base_dir.join(input_file);
        let absolute_path = std::fs::canonicalize(&local_path)
            .map_err(|e| anyhow::anyhow!("Cannot resolve absolute path for {:?}: {}", local_path, e))?;
        let local_path_str = absolute_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid file path: {:?}", absolute_path))?;
        let remote_path = Path::new(input_file).file_name().ok_or_else(|| anyhow!("Invalid filename: {}", input_file))?.to_str().ok_or_else(|| anyhow!("Non-UTF8 filename: {}", input_file))?;
        {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload from Local path: {:?} \n", local_path_str ));
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload to Remote path: {:?} \n", remote_path )); 
        }

        match rust_client.upload_file(local_path_str, &remote_path).await {
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to upload file {}: {}", input_file, e));
            }
        }
    }

    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload of Input and Script Files \n", )); 
    }

    for output_file in &output_files {
        let local_path = base_dir.join(output_file);
        let absolute_path = std::fs::canonicalize(&local_path)
            .map_err(|e| anyhow::anyhow!("Cannot resolve absolute path for {:?}: {}", local_path, e))?;
        let local_path_str = absolute_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid file path: {:?}", absolute_path))?;
        let remote_path = Path::new(output_file).file_name().ok_or_else(|| anyhow!("Invalid filename: {}", output_file))?.to_str().ok_or_else(|| anyhow!("Non-UTF8 filename: {}", output_file))?;
        {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload from Local path: {:?} \n", local_path_str ));
            job_mgr.add_log(job_id, format!("[RustClient Node] Upload to Remote path: {:?} \n", remote_path )); 
        }
        match rust_client.upload_file(local_path_str, &remote_path).await {
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to upload file {}: {}", output_file, e));
            }
        }
    }

    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Job Submission started \n" )); 
    }

    let job_result = rust_client
        .submit_job("job.sh", &project_name, &staged_inputs[..], &output_files[..])
        .await
        .map_err(|e| anyhow!("submit_job failed: {}", e))?;

    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Job Submission Done \n" )); 
    }

    let task_id = match job_result {
        Value::Object(ref map) => map.get("SubmitJob").and_then(|v| v.as_str()),
        Value::String(ref s) => Some(s.as_str()),
        _ => None,
    }.ok_or_else(|| anyhow!("[RustClient] No task ID returned"))?;

    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Job Status loop started\n" )); 
    }

    let _task_json = loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let task_json = match rust_client.get_job(task_id).await {
            Ok(value) => value,
            Err(e) => {
                return Err(anyhow!("RustClient error: {}", e));
            }
        };

        let status = task_json.get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("unknown");

        match status {
            "done" => break task_json,
            "failed" | "error" => return Err(anyhow!("Job failed with status: {}", status)),
            "CompletedWithError" => {
                let error_message = task_json.get("error")
                    .and_then(|e| match e {
                        serde_json::Value::Object(obj) => obj.get("reason").and_then(|v| v.as_str())
                            .or_else(|| obj.get("message").and_then(|v| v.as_str())),
                        _ => e.as_str(),
                    })
                    .or_else(|| task_json.get("stderr").and_then(|v| v.as_str()))
                    .or_else(|| task_json.get("message").and_then(|v| v.as_str()))
                    .or_else(|| task_json.get("details").and_then(|v| v.as_str()))
                    .unwrap_or("Unknown error");

                let log_content = if output_files.contains(&"output.log") {
                    let remote_log_path = "output.log".to_string();
                    let temp_log_path = std::env::temp_dir().join(format!("error_output_{}.log", task_id));

                    match rust_client.download_file(&remote_log_path, temp_log_path.to_str().unwrap()).await {
                        Ok(_) => {
                            let content = std::fs::read_to_string(&temp_log_path)
                                .unwrap_or_else(|e| format!("Could not read downloaded output.log: {}", e));
                            let _ = std::fs::remove_file(&temp_log_path);
                            if content.trim().is_empty() {
                                "output.log file is empty".to_string()
                            } else {
                                content
                            }
                        }
                        Err(e) => format!("Could not download output.log from server: {}", e),
                    }
                } else {
                    "No output.log specified in outputs".to_string()
                };

                let combined_message = format!("Error: {}\nLog content: {}", error_message,
                    if log_content.len() > 1000 {
                        format!("{}... (truncated)", &log_content[..1000])
                    } else {
                        log_content
                    });

                return Err(anyhow!("Job failed with status: CompletedWithError - {}", combined_message));
            }
            _ => continue,
        }
    };
    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Job Status loop Ended \n" )); 
    }
    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Output File Download started \n" )); 
    }

    for &file_name in &output_files {
        let local_path = base_dir.join(file_name);
        let curr_dir = file_name.to_string();
        rust_client.download_file(&curr_dir, local_path.to_str().unwrap()).await
            .map_err(|e| anyhow!("Failed to download {}: {}", file_name, e))?;
    }
    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] Output file download complete \n" )); 
    }
    
    let _ = rust_client.disconnect_ftp().await;

    {
            let mut job_mgr = job_mgr.lock().unwrap();
            job_mgr.add_log(job_id, format!("[RustClient Node] FTP disconnect \n" )); 
    }
    
    Ok(())
}


pub fn action(_states: &mut ui::states::States, store: &data_model::Store) -> anyhow::Result<()> {
    // TODO: currently only support one job
    let job_id = 0;
    {
        let job_mgr = store.job_mgr.lock().unwrap();
        if job_mgr.jobs.contains_key(&job_id) {
            return Err(anyhow::Error::msg("current job already running"));
        }
    }

    let (proj_sel, _) = store.project_sel.as_ref()
        .ok_or(anyhow::Error::msg("no selected project"))?;
    let proj = proj_sel.to_owned();
    let registry_sel = store.registry_mgr.selected(&store.setting_mgr)
        .ok_or(anyhow::Error::msg("no registry selected"))?
        .to_owned();

    let pod_sel = store.pod_mgr.selected()
        .ok_or(anyhow::Error::msg("no pod selected"))?;
    use data_model::pod::Settings;
    match &pod_sel.settings {
        Settings::Local => {
            let job_mgr_clone = store.job_mgr.clone();
            let job_id_to_cancel = store.cancel_job_id.clone();
            let proj_dir = proj.get_dir().to_path_buf();
            let settings_local = proj_sel.get_job_settings()
                .infra_local.as_ref()
                .ok_or(anyhow::Error::msg("no settings for local servers"))?
                .clone();
            tokio::spawn(async move {
                match launch_job_local(job_mgr_clone.clone(), job_id_to_cancel, proj_dir, settings_local).await {
                    Ok(()) => (),
                    Err(e) => {
                        let mut job_mgr = job_mgr_clone.lock().unwrap();
                        job_mgr.add_log(0, format!("run job error: {e}"));
                    } 
                }
            });
        },
        Settings::SakuraInternetServer => { return Err(anyhow::Error::msg("not DOK service")); },
        Settings::SakuraInternetService(_) => {
            let mut job_mgr = store.job_mgr.lock().unwrap();
            job_mgr.add_log(0, "send the job Sakura Internet DOK service ...".to_string());
            let (with_build, param_dok) = super::params::params_dok(store)?;
            if proj.get_job_settings().dok.is_some() && with_build {
                proj.get_dir().join("Dockerfile").exists().then_some(0)
                    .ok_or(anyhow::Error::msg("using DOK service with self built docker image requires a Dockerfile under the project folder"))?;
            }
            let client = store.account_mgr.create_client(&store.setting_mgr)?.clone();
            let job_mgr_clone = store.job_mgr.clone();
            tokio::spawn(async move {
                match launch_job_dok(proj, registry_sel, client, param_dok, job_mgr_clone.clone(), with_build).await {
                    Ok(()) => (),
                    Err(e) => {
                        let mut job_mgr = job_mgr_clone.lock().unwrap();
                        job_mgr.add_log(job_id, format!("[Local infra] job {} exits with error {}", job_id, e));
                    } 
                }
            });
        }
        Settings::RustClient => {
            let mut job_mgr = store.job_mgr.lock().unwrap();
            job_mgr.add_log(0, "send the job to Rust Client service ...".to_string());
            

            let job_mgr_clone = store.job_mgr.clone();
            let proj_clone = proj.clone();

            tokio::spawn(async move {
                let rust_client = match RustClient::from_env().await {
                    Ok(client) => client,
                    Err(e) => {
                        let mut job_mgr = job_mgr_clone.lock().unwrap();
                        job_mgr.add_log(0, format!("Failed to create RustClient: {}", e));
                        return;
                    }
                };

                if let Err(e) = launch_job_rust_client(proj_clone, rust_client, job_mgr_clone.clone()).await {
                    let mut job_mgr = job_mgr_clone.lock().unwrap();
                    job_mgr.add_log(0, format!("RustClient job failed: {}", e));
                }
            });
        }

    };

    Ok(())
}

pub fn render(f: &mut Frame, area: Rect, _states: &mut ui::states::States, store: &data_model::Store) {
    // TODO: use job id 0 for testing first
    let job_id = 0;
    let job_mgr = store.job_mgr.lock().unwrap();
    let mut logs: Vec<Line> = job_mgr.logs.get(&job_id)
        .map(|v| {
            v.iter()
            .map(|s| s.as_str())
            .map(Line::from)
            .collect()
        })
        .unwrap_or_default();
    if let Some(log_tmp) = job_mgr.logs_tmp.get(&job_id) {
        logs.push(Line::from(log_tmp.as_str()));
    }
    let job_logs = Paragraph::new(logs)
        .block(Block::bordered())
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });

    f.render_widget(Clear, area);
    f.render_widget(job_logs, area);
}
