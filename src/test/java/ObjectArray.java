import java.io.Serializable;

/**
 * Created by root on 4/17/17.
 */

public class ObjectArray implements Serializable{
    private static Object[] ObjArr = null;
    private static int flag=0;
    public int num = 0;

    public void setValue(Object[] obj){
        ObjArr=obj;
    }

    public void setFlag(int i)
    {
        flag = i;
    }

    public Object[] getValue(){
        return ObjArr;
    }

    public void setNum(int i) {num = i;}

    public int getNum(){
        return num;
    }



    public int getFlag()
    {
        return flag;
    }
}
