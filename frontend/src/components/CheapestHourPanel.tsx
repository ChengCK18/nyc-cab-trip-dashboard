import { useState, useEffect, ChangeEvent } from "react";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

import locationService from "../services/location";
import tripsServices from "../services/trips";

interface LocationDataType {
    Borough: string;
    Zone: string;
    service_zone: string;
}

interface CheapestHourLocDataType {
    [locationId: string]: LocationDataType;
}

interface CheapestHourEntryType {
    [key: string]: any;
}

const CheapestHourPanel = () => {
    const [selectedDate, setSelectedDate] = useState(new Date());
    const [selectedPOLocation, setSelectedPOLocation] = useState("0");
    const [selectedDOLocation, setSelectedDOLocation] = useState("0");
    const [selectedPassengerCount, setSelectedPassengerCount] =
        useState<number>(0);
    const [selectedTripDistFrom, setSelectedTripDistFrom] = useState<number>(0);
    const [selectedTripDistTo, setSelectedTripDistTo] = useState<number>(0);

    const [loading, setLoading] = useState<boolean>(false);
    const [locationData, setLocationData] = useState<CheapestHourLocDataType>(
        {}
    );
    const [notification, setNotification] = useState<string>("");
    const [cheapestHour, setCheapestHour] = useState<CheapestHourEntryType>([]);

    useEffect(() => {
        getInitialLocData();
    }, []);

    const showNotification = (error: any) => {
        if (
            error.response &&
            error.response.data &&
            error.response.data &&
            error.response.data.message
        ) {
            setNotification(error.response.data.message);

            setTimeout(() => {
                setNotification("");
            }, 5000);
        } else if (typeof error === "string") {
            setNotification(error);
            setTimeout(() => {
                setNotification("");
            }, 5000);
        }
    };

    const getInitialLocData = async () => {
        const response = await locationService.getLocationData();
        if (response instanceof Error) {
            console.log("Response is an error:", response);
        } else {
            setLocationData(response);
        }
    };

    const handleDateChange = (date: Date) => {
        setSelectedDate(date);
    };

    const handleGetCheapestHour = async () => {
        setLoading(true);
        if (selectedPOLocation === "0" || selectedDOLocation === "0") {
            showNotification("Please select pickup and dropoff location");
            setLoading(false);
            return;
        }
        if (
            selectedTripDistFrom > selectedTripDistTo &&
            selectedTripDistTo !== 0
        ) {
            showNotification(
                "Trip distance(From) have to be lower than Trip distance(To)"
            );
            setLoading(false);
            return;
        }

        const year = selectedDate.getFullYear();
        const month = selectedDate.getMonth() + 1;
        const day = selectedDate.getDate();

        const response = await tripsServices.getCheapestHour({
            year,
            month,
            day,
            POloc: selectedPOLocation,
            DOloc: selectedDOLocation,
            passengerCount: selectedPassengerCount,
            tripDistFrom: selectedTripDistFrom,
            tripDistTo: selectedTripDistTo,
        });
        if (response instanceof Error) {
            showNotification(response);
        } else {
            const cheapestHourArray = Object.entries(response);
            cheapestHourArray.sort((a: [string, any], b: [string, any]) => {
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
                return 0;
            });
            setCheapestHour(cheapestHourArray);
        }
        setLoading(false);
    };

    const handlePOLocationChange = (event: ChangeEvent<HTMLSelectElement>) => {
        setSelectedPOLocation(event.target.value);
    };
    const handleDOLocationChange = (event: ChangeEvent<HTMLSelectElement>) => {
        setSelectedDOLocation(event.target.value);
    };

    const handlePassengerCountChange = (
        event: ChangeEvent<HTMLInputElement>
    ) => {
        const inputValue = event.target.value;
        const intValue = parseInt(inputValue);
        if (!isNaN(intValue) && intValue >= 0) {
            setSelectedPassengerCount(intValue);
        } else {
            // If it's not a valid positive integer, set the value to 0
            setSelectedPassengerCount(0);
        }
    };

    const handleTripDistFromChange = (event: ChangeEvent<HTMLInputElement>) => {
        const inputValue = event.target.value;
        const intValue = parseInt(inputValue);
        if (!isNaN(intValue) && intValue >= 0) {
            setSelectedTripDistFrom(intValue);
        } else {
            // If it's not a valid positive integer, set the value to 0
            setSelectedTripDistFrom(0);
        }
    };

    const handleTripDistToChange = (event: ChangeEvent<HTMLInputElement>) => {
        const inputValue = event.target.value;
        const intValue = parseInt(inputValue);
        if (!isNaN(intValue) && intValue >= 0) {
            setSelectedTripDistTo(intValue);
        } else {
            // If it's not a valid positive integer, set the value to 0
            setSelectedTripDistTo(0);
        }
    };

    return (
        <div className="w-1/2 m-6 rounded-2xl bg-white">
            <div className="bg-white rounded-2xl h-1/2 p-5 flex">
                <div className="h-full w-full">
                    {notification !== "" ? (
                        <div className="text-red-500">{notification}</div>
                    ) : (
                        ""
                    )}
                    <p className="p-1">
                        <b>Cheapest hour</b> of the day on average
                    </p>
                    <div>
                        *Date
                        <br />
                        <DatePicker
                            className="border-black border-2 p-1 rounded-2xl transition-transform transform-gpu hover:scale-105 "
                            selected={selectedDate}
                            onChange={handleDateChange}
                            dateFormat="dd/MM/yyyy"
                        />
                    </div>
                    <div className="mt-2">
                        *Pickup Location
                        <br />
                        <select
                            value={selectedPOLocation}
                            onChange={handlePOLocationChange}
                        >
                            <option value="0">Select</option>
                            {Object.keys(locationData).length >= 1 &&
                                Object.entries(locationData).map(
                                    ([locationId, locData]) => {
                                        if (
                                            locData.Borough !== "Unknown" &&
                                            locData.Zone !== "Null"
                                        ) {
                                            return (
                                                <option
                                                    key={locationId}
                                                    value={locationId}
                                                >
                                                    {locationId},{" "}
                                                    {locData.Borough},{" "}
                                                    {locData.Zone}
                                                </option>
                                            );
                                        }
                                    }
                                )}
                        </select>
                    </div>
                    <div className="mt-2">
                        *Dropoff Location
                        <br />
                        <select
                            value={selectedDOLocation}
                            onChange={handleDOLocationChange}
                        >
                            <option value="0">Select</option>
                            {Object.keys(locationData).length >= 1 &&
                                Object.entries(locationData).map(
                                    ([locationId, locData]) => {
                                        if (
                                            locData.Borough !== "Unknown" &&
                                            locData.Zone !== "Null"
                                        ) {
                                            return (
                                                <option
                                                    key={locationId}
                                                    value={locationId}
                                                >
                                                    {locationId},{" "}
                                                    {locData.Borough},{" "}
                                                    {locData.Zone}
                                                </option>
                                            );
                                        }
                                    }
                                )}
                        </select>
                    </div>
                    <div className="mt-3 w-full bg-slate-100 flex flex-row justify-center items-center">
                        <div className="flex flex-col justify-center items-center">
                            Passenger count
                            <br />
                            <input
                                className="m-2"
                                type="number"
                                value={selectedPassengerCount}
                                onChange={handlePassengerCountChange}
                                placeholder="(Optional)Enter an integer"
                            />
                        </div>
                        <div className="flex flex-col justify-center items-center">
                            Trip Distance(From)
                            <br />
                            <input
                                className="m-2"
                                type="number"
                                value={selectedTripDistFrom}
                                onChange={handleTripDistFromChange}
                                placeholder="(Optional)Enter an integer"
                            />
                        </div>
                        <div className="flex flex-col justify-center items-center">
                            Trip Distance(To)
                            <br />
                            <input
                                className="m-2"
                                type="number"
                                value={selectedTripDistTo}
                                onChange={handleTripDistToChange}
                                placeholder="(Optional)Enter an integer"
                            />
                        </div>
                    </div>
                    <div className="mt-3">
                        <button
                            className="bg-blue-200  p-2 rounded-md shadow-md transition-transform transform-gpu hover:scale-105 "
                            onClick={handleGetCheapestHour}
                        >
                            Calculate
                        </button>
                    </div>
                </div>
                {loading ? (
                    <div className="flex flex-grow justify-end items-center">
                        <div className="mr-2 animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-gray-900"></div>
                    </div>
                ) : (
                    ""
                )}

                <div></div>
            </div>
            <div className="h-1/2 overflow-y-scroll">
                <div className="w-full flex justify-center mt-3">
                    <table className="w-3/4 ">
                        <tbody className="border-2">
                            <tr>
                                <th className="border-2">Date And Time</th>
                                <th className="border-2">
                                    Average fare by hour(s)
                                </th>
                            </tr>
                            {cheapestHour.map(
                                (
                                    item: CheapestHourEntryType,
                                    index: number
                                ) => (
                                    <tr
                                        key={item[0]}
                                        className={
                                            index == 0 ? "bg-green-400" : ""
                                        }
                                    >
                                        <td>
                                            {item[0]}
                                            {index == 0 && <b> Cheapest</b>}
                                        </td>
                                        <td>{item[1].toFixed(2)}</td>
                                    </tr>
                                )
                            )}
                            {cheapestHour.length == 0 && (
                                <tr>
                                    <td>No Data Available</td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default CheapestHourPanel;
