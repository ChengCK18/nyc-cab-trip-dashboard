import axios from "axios";
const baseUrl = "http://127.0.0.1:5000/api/trips";

interface TripParams {
    year: number;
    month: number;
    day: number;
}

interface GetCheapestHourType {
    year: number;
    month: number;
    day: number;
    POloc: string;
    DOloc: string;
    passengerCount: number;
    tripDistFrom: number;
    tripDistTo: number;
}

const getTripsByHour = ({ year, month, day }: TripParams) => {
    const params = {
        year: year,
        month: month,
        day: day,
        operation: "q_get_trip_count",
    };
    console.log(params);
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

const getCheapestHour = ({
    year,
    month,
    day,
    POloc,
    DOloc,
    passengerCount,
    tripDistFrom,
    tripDistTo,
}: GetCheapestHourType) => {
    const params = {
        year: year,
        month: month,
        day: day,
        POloc: POloc,
        DOloc: DOloc,
        passengerCount,
        tripDistFrom,
        tripDistTo,
        operation: "q_get_cheapest",
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

const tripsServices = {
    getTripsByHour,
    getCheapestHour,
};

export default tripsServices;
