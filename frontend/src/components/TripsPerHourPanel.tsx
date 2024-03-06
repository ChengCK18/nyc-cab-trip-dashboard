import { useState } from "react";
import tripsServices from "../services/trips";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

interface TripsPerHourData {
    [timestamp: string]: number;
}

const TripsPerHourPanel = () => {
    const [selectedDate, setSelectedDate] = useState(new Date());
    const [tripsPerHour, setTripsPerHour] = useState<TripsPerHourData>({});
    const [loading, setLoading] = useState<boolean>(false);
    const [notification, setNotification] = useState<string>("");

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
        }
    };
    const handleDateChange = (date: Date) => {
        setSelectedDate(date);
    };

    const handleGetTripsByHour = async () => {
        setLoading(true);
        const year = selectedDate.getFullYear();
        const month = selectedDate.getMonth() + 1;
        const day = selectedDate.getDate();

        const response = await tripsServices.getTripsByHour({
            year,
            month,
            day,
        });

        if (response instanceof Error) {
            showNotification(response);
        } else {
            setTripsPerHour(response);
        }
        setLoading(false);
    };
    return (
        <div className="w-1/2 m-6">
            <div className="bg-white h-full p-5 rounded-2xl">
                <div className="flex">
                    <div>
                        {notification !== "" ? (
                            <div className="text-red-500">{notification}</div>
                        ) : (
                            ""
                        )}
                        <p className="p-1">
                            <b>Total trips</b> per hour on{" "}
                            {selectedDate.toDateString()}
                        </p>
                        <div>
                            Date
                            <br />
                            <DatePicker
                                className="border-black border-2 p-1 rounded-2xl transition-transform transform-gpu hover:scale-105 "
                                selected={selectedDate}
                                onChange={handleDateChange}
                                dateFormat="dd/MM/yyyy"
                            />
                        </div>
                        <div className="mt-3">
                            <button
                                className="bg-blue-200  p-2 rounded-md shadow-md transition-transform transform-gpu hover:scale-105 "
                                onClick={handleGetTripsByHour}
                            >
                                Get total trips by hour
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
                <div className="flex mt-3 justify-center">
                    <table className="w-3/4 ">
                        <tbody className="border-2">
                            <tr>
                                <th className="border-2">Date And Time</th>
                                <th className="border-2">Trip Count</th>
                            </tr>
                            {Object.entries(tripsPerHour).map(
                                ([dateTime, count]) => (
                                    <tr key={dateTime}>
                                        <td>{dateTime}</td>
                                        <td>{count}</td>
                                    </tr>
                                )
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default TripsPerHourPanel;
