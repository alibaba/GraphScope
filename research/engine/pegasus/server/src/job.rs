use libloading::{Library, Symbol};
use pegasus::{BuildJobError, Data, Worker};

#[derive(Default)]
pub struct JobDesc {
    pub input: Vec<u8>,
    pub plan: Vec<u8>,
    pub resource: Vec<u8>,
}

impl JobDesc {
    pub fn set_input(&mut self, input_bytes: Vec<u8>) -> &mut Self {
        self.input = input_bytes;
        self
    }

    pub fn set_plan(&mut self, plan_bytes: Vec<u8>) -> &mut Self {
        self.plan = plan_bytes;
        self
    }

    pub fn set_resource(&mut self, resource_bytes: Vec<u8>) -> &mut Self {
        self.resource = resource_bytes;
        self
    }
}

pub trait JobAssembly<I: Data>: Send + Sync + 'static {
    fn assemble(&self, job: &JobDesc, worker: &mut Worker<I, Vec<u8>>) -> Result<(), BuildJobError>;
}

pub struct DynLibraryAssembly;

impl JobAssembly<Vec<u8>> for DynLibraryAssembly {
    fn assemble(&self, job: &JobDesc, worker: &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError> {
        if let Ok(resource) = String::from_utf8(job.resource.clone()) {
            if let Some(lib) = pegasus::resource::get_global_resource::<Library>(&resource) {
                info!("load library {};", resource);
                let func: Symbol<
                    unsafe fn(&[u8], &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError>,
                > = unsafe {
                    match lib.get(&job.plan[..]) {
                        Ok(sym) => sym,
                        Err(e) => {
                            return Err(format!("fail to link, because {:?}", e))?;
                        }
                    }
                };
                unsafe { func(&job.input, worker) }
            } else {
                Err(format!("libarry with name {} not found;", resource))?
            }
        } else {
            Err("illegal library name;")?
        }
    }
}
