import axios from "axios";
const baseUrl = "http://127.0.0.1:5000/api/locations";

const getLocationData = () => {
    const params = {
        operation: "getLocation",
    };
    const request = axios.get(baseUrl, {
        params: params,
        responseType: "json", // Specify responseType as 'json'
    });

    return request
        .then((response) => {
            return response.data;
        })
        .catch((error) => error);
};

const locationService = {
    getLocationData,
};

export default locationService;
