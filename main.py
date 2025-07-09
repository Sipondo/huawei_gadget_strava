from fit_tool.fit_file import FitFile


# path = r'C:\Users\tt_ro\Nextcloud\Gadgetbridge\files'
path = "Lunch_Swim.fit"
fit_file = FitFile.from_file(path)

out_path = "test.csv"
fit_file.to_csv(out_path)

for record in fit_file.records:
    message = record.message
    if message.global_id == SessionMessage.ID:
        print(message.to_row())
        # print(message.get_field_definition(SessionSportField.ID)))


# from fit_tool.profile.messages.record_message import RecordMessage, RecordHeartRateField, RecordPowerField
# from fit_tool.profile.messages.activity_message import ActivityMessage

from fit_tool.profile.messages.session_message import SessionMessage, SessionSportField

SessionMessage.ID
# ActivityMessage.ID
# RecordMessage.ID


# from fit_tool.fit_file import FitFile
# from fit_tool.fit_file_builder import FitFileBuilder
# from fit_tool.profile.messages.record_message import RecordMessage, RecordHeartRateField, RecordPowerField


# def main():
#     """The following program reads all the bytes from a FIT formatted file and then
#         decodes these bytes to create a FIT file object. We then build a modified FIT file
#         based on a variety of criteria (see comments below). Finally we output
#         the modified data to a new FIT file.
#     """
#     path = '../tests/data/sdk/Activity.fit'
#     fit_file = FitFile.from_file(path)

#     builder = FitFileBuilder(auto_define=False)

#     for record in fit_file.records:
#         message = record.message
#         include_record = True

#         if message.global_id == RecordMessage.ID:
#             # Remove the heart rate field from all record definition and data messages
#             message.remove_field(RecordHeartRateField.ID)

#             if isinstance(message, RecordMessage):
#                 # remove records where the power is too high
#                 power_field = message.get_field(RecordPowerField.ID)
#                 if power_field and power_field.is_valid():
#                     power = power_field.get_value()
#                     if power > 800:
#                         include_record = False

#         if include_record:
#             builder.add(message)

#     modified_file = builder.build()
#     modified_file.to_file('../tests/out/modified_activity.fit')

#     fit_file2 = FitFile.from_file('../tests/out/modified_activity.fit')


# if __name__ == "__main__":
#     main()
