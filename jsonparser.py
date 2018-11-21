import json
import requests



"""Class object for Json parsing from Url or file or even data can pass to this
   and can extract data till second level 
"""

class JsonParser(object):
    def __init__(self,val,type='url'):
        if type=='url':
            self.url=val
            self.urlload()
        if type=='singleobject_file':
            self.filename=val
            self.singleobject_fileload()

        if type=='multiobject_file':
            self.filename=val
            self.multiobject_fileload()

        if type=='data':
            self.data=json.loads(val)






    def urlload(self):
        print self.url
        resp = requests.get(self.url,verify=False)
        self.data = json.loads(resp.text)
        return resp

    def multiobject_fileload(self):
        self.data = []
        with open(self.filename) as f:
            for line in f:

                self.data.append(json.loads(line))
        return self.data

    def singleobject_fileload(self):
        with open(self.filename) as data_file:
            self.data = json.load(data_file)
        return self.data



    def jsonFetchValues(self, lst):

        value_dict = {};
        output_list = []


        if isinstance(lst, str):

            for line_no, input_data in enumerate(self.data):

                output_dict = {}



                output_dict[lst] = input_data.get(str(lst),None)
                output_dict['line_no'] = line_no
                output_list.append(output_dict)



        else:

            for line_no, input_data in enumerate(self.data):

                output_dict = {}


                for key,value in enumerate(lst):
                    output_dict[value] = input_data.get(str(value),None)
                    output_dict['line_no'] = line_no
                output_list.append(output_dict)


        return output_list;

    def jsonMultiFetchValues(self, input_key, input_values,input_full_data=''):

        if input_full_data=='':
            input_full_data=self.data

        val = ''

        if isinstance(input_values, str):
            val = input_values
        if isinstance(input_values, list):
            if len(input_values) == 1:
                val = input_values[0]


        output_list = []

        for line_no,input_data in enumerate(input_full_data):

            if isinstance(input_data, dict):

                print ' Json Data line : '+str(line_no)
                print input_data

                extracted_value = input_data.get(input_key,0);

                print ' Extracted Data line :'+str(line_no)
                print extracted_value

                if isinstance(extracted_value, list):
                    print ' Extracted value  is  list inside dict Json data'


                    if val != '':
                        print ' what is this what is val '+str(val)
                        output_dict = {}
                        for i in extracted_value:
                            output_dict[str(val)] = i.get(str(val), None)
                            output_dict['line_no']=line_no
                            output_list.append(output_dict)
                    else:

                        output_dict = {}

                        for i in extracted_value:
                            output_dict = {}

                            for j, value in enumerate(input_values):

                                output_dict[str(value)] = i.get(str(value),None)
                            output_dict['line_no'] = line_no
                            output_list.append(output_dict)


                if isinstance(extracted_value, dict):
                    print 'value  is a dict inside dict Json data'
                    if val != '':
                        output_list = extracted_value.get(val,None)
                    else:
                        output_dict = {}
                        for j, value in enumerate(input_values):

                            output_dict[str(value)] = extracted_value.get(value,None)
                        output_list.append(output_dict)

            if isinstance(input_data, list):
                print ' json data is list'
                if val != '':
                    for i in self.data:
                        output_list.append(i[val])
                else:

                    output_dict = {}
                    value_dict = self.data
                    for i in value_dict:
                        output_dict = {}

                        for j, value in enumerate(input_values):
                            output_dict[str(value)] = i.get(str(value),0)
                        output_list.append(output_dict)



            else:
                pass

        return output_list

    def flatten_json(self,input_data):
        output_data = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a,name)
                    i += 1
            else:
                output_data[str(name[:-1])] = str(x)

        flatten(input_data)
        return output_data
