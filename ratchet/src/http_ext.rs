use crate::errors::Error;
use crate::Request;
use http::Uri;
use url::Url;

pub trait TryIntoRequest {
    fn try_into_request(self) -> Result<Request, Error>;
}

impl<'a> TryIntoRequest for &'a str {
    fn try_into_request(self) -> Result<Request, Error> {
        self.parse::<Uri>()?.try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a String {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for String {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl<'a> TryIntoRequest for &'a Uri {
    fn try_into_request(self) -> Result<Request, Error> {
        self.clone().try_into_request()
    }
}

impl TryIntoRequest for Uri {
    fn try_into_request(self) -> Result<Request, Error> {
        Ok(Request::get(self).body(())?)
    }
}

impl<'a> TryIntoRequest for &'a Url {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Url {
    fn try_into_request(self) -> Result<Request, Error> {
        self.as_str().try_into_request()
    }
}

impl TryIntoRequest for Request {
    fn try_into_request(self) -> Result<Request, Error> {
        Ok(self)
    }
}
